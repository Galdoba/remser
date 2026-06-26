// Package client provides a configurable WebSocket client for executing commands
// on a remote server with optional SSH tunneling support.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/gorilla/websocket"
)

// Internal sentinel error for normal task completion.
// Use IsTaskFinished to check for this condition.
var ErrTaskFinished = errors.New("task finished normally")

const (
	// Default durations for keep‑alive and timeouts.
	defaultPongTimeout         = 90 * time.Second
	defaultActivePingDuration  = 3 * time.Second
	defaultPassivePingDuration = 30 * time.Second
	defaultWriteTimeout        = 10 * time.Second

	// Buffer size for reading stdin.
	stdinBufferSize = 1024

	// WebSocket scheme strings.
	schemeHTTP  = "http"
	schemeHTTPS = "https"
	schemeWS    = "ws"
	schemeWSS   = "wss"
	defaultPath = "/ws"

	// Default SSH port.
	defaultSSHPort = "22"

	// Message type constants.
	msgTypeOutput  = "output"
	msgTypeSystem  = "system"
	msgTypeInput   = "input"
	msgTypeExecute = "execute"

	// System event constants.
	eventQueuePosition = "queue_position"
	eventTaskStarted   = "task_started"
	eventTaskFinished  = "task_finished"

	// Close message text sent when the task finishes.
	closeNormalText = "task completed"
)

// Client is a configurable WebSocket client that connects to a remote server,
// sends a command for execution, and optionally provides interactive stdin.
//
// All heavy dependencies (logger, dialer, I/O streams) are injected via
// functional options. The client supports SSH tunneling when configured
// with WithSSHTunnel.
type Client struct {
	// URL is the full WebSocket server address (ws/wss scheme).
	// It may be supplied as http/https and will be converted automatically.
	URL string

	// ClientID is an identifier sent to the server with every execution request.
	ClientID string

	// Interactive enables forwarding of stdin to the remote command.
	Interactive bool

	// Stdin is the source for interactive input (defaults to os.Stdin).
	Stdin io.Reader

	// Stdout receives the command's output.
	Stdout io.Writer

	// Stderr receives system messages, errors, and diagnostics.
	Stderr io.Writer

	// Dialer is a custom WebSocket dialer (timeouts, headers).
	Dialer *websocket.Dialer

	// Logger is the structured logger used by the client.
	Logger *slog.Logger

	// OnMessage is an optional callback that receives every incoming message.
	// When nil, a built‑in default handler is used.
	OnMessage func(msg models.ClientMessage)

	// Internal settings for keep‑alive and timeouts.
	pingInterval time.Duration
	pongTimeout  time.Duration
	writeTimeout time.Duration

	// SSH tunnel configuration (populated by WithSSHTunnel).
	useSSH bool
	sshCfg config.SSH
}

// NewClient creates a new Client with the given server URL and options.
// The URL may use http/https or ws/wss scheme; the former is converted
// automatically. If the path is empty, "/ws" is appended.
func NewClient(serverURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	// Normalise the scheme to WebSocket.
	switch u.Scheme {
	case schemeHTTP:
		u.Scheme = schemeWS
	case schemeHTTPS:
		u.Scheme = schemeWSS
	case schemeWS, schemeWSS:
		// valid
	default:
		return nil, fmt.Errorf("unsupported url scheme: %s", u.Scheme)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = defaultPath
	}

	c := &Client{
		URL:          u.String(),
		Stdin:        os.Stdin,
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		Dialer:       websocket.DefaultDialer,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, nil)),
		pingInterval: defaultPassivePingDuration,
		pongTimeout:  defaultPongTimeout,
		writeTimeout: defaultWriteTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// ---------- High‑level execution ----------

// Execute connects to the server, sends the command described by args,
// and processes messages until the task finishes or the context is cancelled.
func (c *Client) Execute(ctx context.Context, args []string) error {
	dialer, sshClient, err := c.setupSSHTunnelIfNeeded()
	if err != nil {
		return fmt.Errorf("ssh tunnel: %w", err)
	}
	if sshClient != nil {
		defer sshClient.Close()
	}

	conn, _, err := dialer.DialContext(ctx, c.URL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	pingCh := make(chan time.Duration, 1)
	c.configureConnectionHandlers(conn, pingCh)

	go c.pingLoop(ctx, conn, pingCh)

	if err := c.sendExecuteRequest(conn, args); err != nil {
		return err
	}

	if c.Interactive {
		stdinCtx, cancelStdin := context.WithCancel(ctx)
		defer cancelStdin()
		go c.readStdinLoop(stdinCtx, conn, cancelStdin)
	}

	return c.readMessageLoop(ctx, conn, pingCh)
}

// ---------- WebSocket helpers ----------

// configureConnectionHandlers sets the ping/pong handlers and initial read deadline
// on the WebSocket connection. The pingCh channel is used to dynamically update
// the client‑side ping interval.
func (c *Client) configureConnectionHandlers(conn *websocket.Conn, pingCh chan<- time.Duration) {
	conn.SetPingHandler(func(appData string) error {
		c.Logger.Debug("server ping received")
		deadline := time.Now().Add(c.pongTimeout)
		conn.SetReadDeadline(deadline)
		return nil // pong is sent automatically
	})

	conn.SetPongHandler(func(appData string) error {
		c.Logger.Debug("pong received")
		deadline := time.Now().Add(c.pongTimeout)
		conn.SetReadDeadline(deadline)
		return nil
	})

	conn.SetReadDeadline(time.Now().Add(c.pongTimeout))
}

// sendExecuteRequest builds and sends the initial "execute" command over the WebSocket.
func (c *Client) sendExecuteRequest(conn *websocket.Conn, args []string) error {
	req := models.WSCommand{
		Type:        msgTypeExecute,
		Args:        args,
		ClientID:    c.ClientID,
		Interactive: c.Interactive,
	}
	return c.writeJSON(conn, req)
}

// readMessageLoop reads incoming JSON messages and dispatches them until the
// context is cancelled or the task finishes. It also updates the ping interval
// based on system events.
func (c *Client) readMessageLoop(ctx context.Context, conn *websocket.Conn, pingCh chan<- time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var msg models.ClientMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.Logger.Warn("ws read error", "err", err)
				return fmt.Errorf("read message: %w", err)
			}
			return nil
		}

		// Adjust the ping frequency when the server signals state changes.
		c.updatePingInterval(msg, pingCh)

		if msg.Type == msgTypeSystem && msg.Event == eventTaskFinished {
			closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, closeNormalText)
			_ = c.writeControl(conn, websocket.CloseMessage, closeMsg)
			return ErrTaskFinished
		}

		if c.OnMessage != nil {
			c.OnMessage(msg)
		} else {
			c.defaultHandler(msg)
		}
	}
}

// ---------- Keep‑alive ----------

// updatePingInterval adjusts the client ping interval based on the server
// message (queue_position → passive, task_started → active, task_finished → 0).
func (c *Client) updatePingInterval(msg models.ClientMessage, pingCh chan<- time.Duration) {
	if msg.Type != msgTypeSystem {
		return
	}
	switch msg.Event {
	case eventQueuePosition:
		nonBlockingSend(pingCh, defaultPassivePingDuration)
	case eventTaskStarted:
		nonBlockingSend(pingCh, defaultActivePingDuration)
	case eventTaskFinished:
		nonBlockingSend(pingCh, 0)
	}
}

// nonBlockingSend attempts to send a value on ch without blocking.
func nonBlockingSend(ch chan<- time.Duration, v time.Duration) {
	select {
	case ch <- v:
	default:
	}
}

// pingLoop runs the client‑side ping mechanism. It reads the desired interval
// from pingCh and creates/tears down a ticker accordingly.
func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn, pingCh <-chan time.Duration) {
	var ticker *time.Ticker
	var tickerCh <-chan time.Time

	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case newInterval := <-pingCh:
			// Replace the current ticker.
			if ticker != nil {
				ticker.Stop()
				ticker = nil
				tickerCh = nil
			}
			if newInterval > 0 {
				ticker = time.NewTicker(newInterval)
				tickerCh = ticker.C
			}
		case <-tickerCh:
			if err := c.writeControl(conn, websocket.PingMessage, nil); err != nil {
				c.Logger.Error("ping failed", "error", err)
				return
			}
		}
	}
}

// ---------- Low‑level I/O ----------

// writeJSON marshals v as JSON and writes it to the WebSocket connection
// with the configured write timeout.
func (c *Client) writeJSON(conn *websocket.Conn, v any) error {
	conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return conn.WriteJSON(v)
}

// writeControl sends a WebSocket control message (ping, pong, close) with
// the configured write timeout.
func (c *Client) writeControl(conn *websocket.Conn, messageType int, data []byte) error {
	conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return conn.WriteMessage(messageType, data)
}

// ---------- Stdin forwarding ----------

// readStdinLoop reads from the configured Stdin and sends each chunk as an
// "input" message. If a fatal error occurs, it cancels the parent context to
// terminate the main execution loop.
func (c *Client) readStdinLoop(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc) {
	buf := make([]byte, stdinBufferSize)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := c.Stdin.Read(buf)
		if n > 0 {
			input := models.WSCommand{
				Type:  msgTypeInput,
				Input: string(buf[:n]),
			}
			if err := c.writeJSON(conn, input); err != nil {
				c.Logger.Error("write input failed", "err", err)
				cancel()
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				c.Logger.Error("stdin read error", "err", err)
				cancel()
			}
			return
		}
	}
}

// ---------- Default message handler ----------

// defaultHandler implements built‑in output and system message handling when
// no OnMessage callback is provided.
func (c *Client) defaultHandler(msg models.ClientMessage) {
	switch msg.Type {
	case msgTypeOutput:
		fmt.Fprint(c.Stdout, msg.Data)
		if msg.Delim != "" {
			fmt.Fprint(c.Stdout, msg.Delim)
		} else {
			// Ensure output ends with a newline unless the delimiter already provides one.
			if len(msg.Data) > 0 && msg.Data[len(msg.Data)-1] != '\n' {
				fmt.Fprintln(c.Stdout)
			}
		}
	case msgTypeSystem:
		switch msg.Event {
		case eventQueuePosition:
			fmt.Fprintf(c.Stderr, "[queue] position: %d\r", msg.Pos)
		case eventTaskStarted:
			fmt.Fprintln(c.Stderr, "[queue] a task started executing")
		default:
			fmt.Fprintf(c.Stderr, "[system] event: %s\n", msg.Event)
		}
	default:
		fmt.Fprintf(c.Stderr, "[client] unknown message type: %s\n", msg.Type)
	}
}
