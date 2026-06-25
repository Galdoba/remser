package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/Galdoba/remser/internal/server/queue"
	"github.com/gorilla/websocket"
)

// ------------------ Mock QueueManager ------------------

type mockQueueManager struct {
	registered  map[string]chan<- models.ClientMessage
	mu          sync.Mutex
	feedInputCh chan inputData
	enqueueCh   chan *queue.Task
}

type inputData struct {
	clientID string
	data     []byte
}

func newMockQueueManager() *mockQueueManager {
	return &mockQueueManager{
		registered:  make(map[string]chan<- models.ClientMessage),
		feedInputCh: make(chan inputData, 10),
		enqueueCh:   make(chan *queue.Task, 1),
	}
}

// новый метод RegisterSession
func (m *mockQueueManager) RegisterSession(sess queue.ClientSession) {
	m.mu.Lock()
	m.registered[sess.ID()] = sess.Outbox()
	m.mu.Unlock()
}

// убрать старый метод
func (m *mockQueueManager) UnregisterSession(clientID string) {
	m.mu.Lock()
	delete(m.registered, clientID)
	m.mu.Unlock()
}

func (m *mockQueueManager) FeedInput(clientID string, input []byte) error {
	select {
	case m.feedInputCh <- inputData{clientID, input}:
	default:
	}
	return nil
}

func (m *mockQueueManager) Enqueue(task *queue.Task) {
	select {
	case m.enqueueCh <- task:
	default:
	}
}

// ------------------ WebSocket Test Helpers ------------------

func launchTestServer(t *testing.T, maxConn int64) (*httptest.Server, *TaskHandler, *mockQueueManager) {
	t.Helper()
	mockQM := newMockQueueManager()
	logger := slog.Default()
	cfg := config.ServerCFG{MaxConnections: maxConn}
	handler := NewTaskHandler(cfg, mockQM, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handler.WebSocketHandler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, handler, mockQM
}

func connectWS(t *testing.T, url string, firstMsg models.WSCommand) *websocket.Conn {
	t.Helper()
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	if err := conn.WriteJSON(firstMsg); err != nil {
		conn.Close()
		t.Fatalf("write handshake: %v", err)
	}
	return conn
}

func readJSONWithTimeout(t *testing.T, conn *websocket.Conn, timeout time.Duration) (models.ClientMessage, error) {
	t.Helper()
	msgCh := make(chan models.ClientMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		var msg models.ClientMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			errCh <- err
		} else {
			msgCh <- msg
		}
	}()
	select {
	case msg := <-msgCh:
		return msg, nil
	case err := <-errCh:
		return models.ClientMessage{}, err
	case <-time.After(timeout):
		return models.ClientMessage{}, errors.New("read timeout")
	}
}

// ------------------ Tests ------------------

func TestNewTaskHandler(t *testing.T) {
	mockQM := newMockQueueManager()
	cfg := config.ServerCFG{MaxConnections: 5}
	logger := slog.Default()
	h := NewTaskHandler(cfg, mockQM, logger)
	if h.maxConn != 5 {
		t.Errorf("maxConn = %d, want 5", h.maxConn)
	}
	if h.activeConn.Load() != 0 {
		t.Errorf("activeConn should be 0 initially")
	}
}

func TestWebSocketHandler_MaxConnections(t *testing.T) {
	srv, _, _ := launchTestServer(t, 1)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	conn1 := connectWS(t, wsURL, models.WSCommand{
		Type:        models.WSCmdExecute,
		ClientID:    "client-1",
		Interactive: false,
		Args:        []string{"test"},
	})
	defer conn1.Close()

	dialer := websocket.DefaultDialer
	_, resp, err := dialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected error due to max connections")
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %v", resp)
	}
}

func TestWebSocketHandler_FullLifecycle(t *testing.T) {
	srv, _, mockQM := launchTestServer(t, 10)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	conn := connectWS(t, wsURL, models.WSCommand{
		Type:        models.WSCmdExecute,
		ClientID:    "lifecycle-client",
		Interactive: true,
		Args:        []string{"cmd"},
	})
	defer conn.Close()

	select {
	case task := <-mockQM.enqueueCh:
		if !strings.HasPrefix(task.SessionID, "lifecycle-client-") {
			t.Errorf("task ClientID = %s, want prefix lifecycle-client-", task.SessionID)
		}
		outMsg := models.ClientMessage{
			Type: "task_output",
			Data: "hello from queue",
		}
		mockQM.mu.Lock()
		outbox, ok := mockQM.registered[task.SessionID]
		mockQM.mu.Unlock()
		if !ok {
			t.Fatal("client not registered")
		}
		outbox <- outMsg
		close(outbox)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for enqueued task")
	}

	msg1, err := readJSONWithTimeout(t, conn, time.Second)
	if err != nil {
		t.Fatalf("read msg1: %v", err)
	}
	if msg1.Type != "task_output" || msg1.Data != "hello from queue" {
		t.Errorf("unexpected msg1: %+v", msg1)
	}
	msg2, err := readJSONWithTimeout(t, conn, time.Second)
	if err != nil {
		t.Fatalf("read msg2: %v", err)
	}
	if msg2.Type != "system" || msg2.Event != "task_finished" {
		t.Errorf("unexpected msg2: %+v", msg2)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Error("expected connection close after task_finished")
	}
}

func TestWebSocketHandler_InvalidHandshake(t *testing.T) {
	srv, _, _ := launchTestServer(t, 10)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	badMsg := models.WSCommand{Type: "garbage"}
	conn.WriteJSON(badMsg)

	msg, err := readJSONWithTimeout(t, conn, time.Second)
	if err != nil {
		t.Fatalf("expected error message, got read error: %v", err)
	}
	if msg.Type != "system" || msg.Event != "error" {
		t.Errorf("expected system/error, got %+v", msg)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Error("expected connection close after invalid handshake")
	}
}

func TestWebSocketHandler_FeedInput(t *testing.T) {
	srv, _, mockQM := launchTestServer(t, 10)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	conn := connectWS(t, wsURL, models.WSCommand{
		Type:        models.WSCmdExecute,
		ClientID:    "input-client",
		Interactive: true,
		Args:        []string{"interactive-cmd"},
	})
	defer conn.Close()

	inputCmd := models.WSCommand{Type: models.WSCmdInput, Input: "user typed text"}
	if err := conn.WriteJSON(inputCmd); err != nil {
		t.Fatal(err)
	}

	select {
	case in := <-mockQM.feedInputCh:
		if string(in.data) != "user typed text" {
			t.Errorf("unexpected input data: %s", in.data)
		}
		if !strings.HasPrefix(in.clientID, "input-client-") {
			t.Errorf("clientID prefix mismatch: %s", in.clientID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for FeedInput")
	}
}

// ------------------ Unit-тесты Router ------------------

func TestRouter_OutboxMessage(t *testing.T) {
	inbox := make(chan models.WSCommand)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.Default()
	cfg := RouterConfig{
		OutboxSize: 10,
		Inbox:      inbox,
		Ctx:        ctx,
		Logger:     log,
	}
	router := NewRouter(cfg)

	var received models.ClientMessage
	wg := sync.WaitGroup{}
	wg.Add(1)
	router.SetOutboxHandler(func(msg models.ClientMessage) {
		received = msg
		wg.Done()
	})
	router.SetCommandHandler(nil)
	router.SetOutboxClosedHandler(nil)

	go router.Run()

	router.Outbox() <- models.ClientMessage{Type: "test", Data: "data"}
	wg.Wait()
	if received.Type != "test" || received.Data != "data" {
		t.Errorf("unexpected message: %+v", received)
	}
	cancel()
}

func TestRouter_OutboxClosed(t *testing.T) {
	inbox := make(chan models.WSCommand)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.Default()
	cfg := RouterConfig{
		OutboxSize: 10,
		Inbox:      inbox,
		Ctx:        ctx,
		Logger:     log,
	}
	router := NewRouter(cfg)

	closed := make(chan struct{})
	router.SetOutboxHandler(func(msg models.ClientMessage) {})
	router.SetOutboxClosedHandler(func() { close(closed) })

	go router.Run()
	close(router.Outbox())
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("outbox closed handler not called")
	}
}

func TestRouter_InboxMessage(t *testing.T) {
	inbox := make(chan models.WSCommand, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.Default()
	cfg := RouterConfig{
		OutboxSize: 10,
		Inbox:      inbox,
		Ctx:        ctx,
		Logger:     log,
	}
	router := NewRouter(cfg)

	var received models.WSCommand
	wg := sync.WaitGroup{}
	wg.Add(1)
	router.SetCommandHandler(func(cmd models.WSCommand) {
		received = cmd
		wg.Done()
	})

	go router.Run()
	inbox <- models.WSCommand{Type: "custom", Input: "hello"}
	wg.Wait()
	if received.Type != "custom" || received.Input != "hello" {
		t.Errorf("unexpected command: %+v", received)
	}
	cancel()
}

func TestRouter_InboxClosedHandler(t *testing.T) {
	inbox := make(chan models.WSCommand)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.Default()
	cfg := RouterConfig{
		OutboxSize: 10,
		Inbox:      inbox,
		Ctx:        ctx,
		Logger:     log,
	}
	router := NewRouter(cfg)

	closed := make(chan struct{})
	router.SetInboxClosedHandler(func() { close(closed) })

	go router.Run()
	close(inbox)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("inbox closed handler not called")
	}
}

func TestRouter_ContextCancellation(t *testing.T) {
	inbox := make(chan models.WSCommand)
	ctx, cancel := context.WithCancel(context.Background())
	log := slog.Default()
	cfg := RouterConfig{
		OutboxSize: 10,
		Inbox:      inbox,
		Ctx:        ctx,
		Logger:     log,
	}
	router := NewRouter(cfg)

	exited := make(chan struct{})
	go func() {
		router.Run()
		close(exited)
	}()
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("router didn't exit on context cancel")
	}
}

// ------------------ Unit-тесты InteractiveHandshake ------------------

func TestInteractiveHandshake_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, _ := upgrader.Upgrade(w, r, nil)
		defer conn.Close()
		sess := newSession(context.Background(), conn, slog.Default())
		handshaker := NewInteractiveHandshake(time.Second, slog.Default())
		clientID, extra, err := handshaker.Handshake(sess, r)
		resp := map[string]interface{}{"clientID": clientID, "err": err != nil}
		if extra != nil {
			resp["extra"] = extra.(*models.WSCommand).Type
		}
		conn.WriteJSON(resp)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.WriteJSON(models.WSCommand{Type: models.WSCmdExecute, ClientID: "alice", Args: []string{"a"}})
	var resp map[string]interface{}
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatal(err)
	}
	cid, ok := resp["clientID"].(string)
	if !ok || !strings.HasPrefix(cid, "alice-") {
		t.Errorf("clientID = %v, want prefix alice-", resp["clientID"])
	}
	if resp["err"] != false {
		t.Errorf("expected no error")
	}
}

func TestInteractiveHandshake_WrongType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, _ := upgrader.Upgrade(w, r, nil)
		defer conn.Close()
		sess := newSession(context.Background(), conn, slog.Default())
		handshaker := NewInteractiveHandshake(time.Second, slog.Default())
		_, _, err := handshaker.Handshake(sess, r)
		if err != nil {
			conn.WriteJSON(models.ClientMessage{Type: "system", Event: "error"})
		} else {
			conn.WriteJSON(models.ClientMessage{Type: "system", Event: "ok"})
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn.Close()

	conn.WriteJSON(models.WSCommand{Type: "bad"})
	var msg models.ClientMessage
	conn.ReadJSON(&msg)
	if msg.Event != "error" {
		t.Errorf("expected error event, got %v", msg)
	}
}

func TestInteractiveHandshake_MissingClientID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, _ := upgrader.Upgrade(w, r, nil)
		defer conn.Close()
		sess := newSession(context.Background(), conn, slog.Default())
		handshaker := NewInteractiveHandshake(time.Second, slog.Default())
		clientID, _, _ := handshaker.Handshake(sess, r)
		conn.WriteJSON(map[string]string{"clientID": clientID})
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn.Close()

	conn.WriteJSON(models.WSCommand{Type: models.WSCmdExecute}) // ClientID пусто
	var resp map[string]string
	conn.ReadJSON(&resp)
	if resp["clientID"] == "" {
		t.Error("expected non-empty clientID (IP)")
	}
}

// ------------------ Unit-тесты Session ------------------

func TestSession_Close(t *testing.T) {
	upg := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upg.Upgrade(w, r, nil)
		sess := newSession(context.Background(), conn, slog.Default())
		defer sess.Close()
		<-sess.ctx.Done()
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	if err != nil {
		// нормально
	}
}

func TestSession_Reader(t *testing.T) {
	upg := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upg.Upgrade(w, r, nil)
		sess := newSession(context.Background(), conn, slog.Default())
		cmds := make(chan models.WSCommand, 1)
		sess.StartReader(cmds)
		select {
		case cmd := <-cmds:
			conn.WriteJSON(cmd)
			sess.cancel()
		case <-time.After(time.Second):
		}
		<-sess.ReaderDone()
		conn.Close()
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, _ := websocket.DefaultDialer.Dial(wsURL, nil)
	defer conn.Close()
	conn.WriteJSON(models.WSCommand{Type: "input", Input: "test"})
	var echoed models.WSCommand
	if err := conn.ReadJSON(&echoed); err != nil {
		t.Fatal(err)
	}
	if echoed.Input != "test" {
		t.Errorf("expected 'test', got %v", echoed)
	}
}
