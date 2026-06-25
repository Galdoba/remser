package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/gorilla/websocket"
)

const (
	defaultPongTimeout         = time.Duration(time.Second * 90)
	defaultactivePingDuration  = time.Duration(time.Second * 3)
	defaultpassivePingDuration = time.Duration(time.Second * 30)
)

var (
	// ErrTaskFinished возвращается, если сервер корректно завершил задачу.
	ErrTaskFinished = fmt.Errorf("task finished normally")
)

// Client представляет конфигурируемого WebSocket-клиента для выполнения команд.
type Client struct {
	// URL полный адрес WebSocket-сервера (схема ws/wss). Может быть передан http/https – схема будет исправлена.
	URL string
	// ClientID идентификатор клиента, отправляемый серверу.
	ClientID string
	// Interactive включает режим интерактивного ввода (stdin пересылается на сервер).
	Interactive bool

	// Stdin источник данных для интерактивного ввода (по умолчанию os.Stdin).
	Stdin io.Reader
	// Stdout приёмник для вывода команды.
	Stdout io.Writer
	// Stderr приёмник для системных сообщений и ошибок.
	Stderr io.Writer

	// Dialer кастомный WebSocket dialer (таймауты, заголовки).
	Dialer *websocket.Dialer
	// Logger структурированный логгер.
	Logger *slog.Logger

	// OnMessage опциональный обработчик всех входящих сообщений.
	// Если nil, используется defaultHandler.
	OnMessage func(msg models.ClientMessage)

	// Внутренние настройки
	pingInterval time.Duration
	pongTimeout  time.Duration
	writeTimeout time.Duration

	pingIntervalCh chan time.Duration
}

// Option позволяет гибко настраивать Client.
type Option func(*Client)

// WithStdin задаёт io.Reader для интерактивного ввода.
func WithStdin(r io.Reader) Option {
	return func(c *Client) { c.Stdin = r }
}

// WithStdout переопределяет поток stdout.
func WithStdout(w io.Writer) Option {
	return func(c *Client) { c.Stdout = w }
}

// WithStderr переопределяет поток stderr.
func WithStderr(w io.Writer) Option {
	return func(c *Client) { c.Stderr = w }
}

// WithLogger устанавливает slog.Logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.Logger = l }
}

// WithDialer позволяет использовать кастомный websocket.Dialer.
func WithDialer(d *websocket.Dialer) Option {
	return func(c *Client) { c.Dialer = d }
}

// WithClientID задаёт идентификатор клиента.
func WithClientID(id string) Option {
	return func(c *Client) { c.ClientID = id }
}

// WithInteractive включает интерактивный режим.
func WithInteractive(interactive bool) Option {
	return func(c *Client) { c.Interactive = interactive }
}

// WithPingInterval задаёт интервал отправки клиентских ping-фреймов.
// Если 0, клиент не шлёт ping самостоятельно, но отвечает на серверные pong.
func WithPingInterval(d time.Duration) Option {
	return func(c *Client) { c.pingInterval = d }
}

// WithWriteTimeout задаёт таймаут на запись сообщения.
func WithWriteTimeout(d time.Duration) Option {
	return func(c *Client) { c.writeTimeout = d }
}

// WithPongTimeout задаёт максимальное время ожидания pong-ответа от сервера.
func WithPongTimeout(d time.Duration) Option {
	return func(c *Client) { c.pongTimeout = d }
}

// NewClient создаёт клиент с обязательным URL сервера и опциями.
// URL может быть в формате http(s)://host:port/path или ws(s)://host:port/path.
// Если путь пуст, добавляется стандартный "/ws".
func NewClient(serverURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("unsupported url scheme: %s", u.Scheme)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}

	c := &Client{
		URL:          u.String(),
		Stdin:        os.Stdin,
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		Dialer:       websocket.DefaultDialer,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, nil)),
		pingInterval: defaultpassivePingDuration,
		pongTimeout:  defaultPongTimeout,
		writeTimeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (c *Client) Execute(ctx context.Context, args []string) error {
	conn, _, err := c.Dialer.DialContext(ctx, c.URL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	c.pingIntervalCh = make(chan time.Duration, 1)

	// Обработчик входящих ping от сервера (если будут)
	conn.SetPingHandler(func(appData string) error {
		c.Logger.Debug("server ping received")
		deadline := time.Now().Add(c.pongTimeout)
		conn.SetReadDeadline(deadline)
		return nil // pong отправится автоматически
	})

	// Обработчик pong на наши ping – продлевает deadline
	conn.SetPongHandler(func(appData string) error {
		c.Logger.Debug("pong received")
		deadline := time.Now().Add(c.pongTimeout)
		conn.SetReadDeadline(deadline)
		return nil
	})

	// Начальный deadline
	conn.SetReadDeadline(time.Now().Add(c.pongTimeout))

	// Запускаем keep-alive (клиентские ping)
	go c.pingLoop(ctx, conn)

	req := models.WSCommand{
		Type:        "execute",
		Args:        args,
		ClientID:    c.ClientID,
		Interactive: c.Interactive,
	}
	if err := c.writeJSON(conn, req); err != nil {
		return fmt.Errorf("send execute: %w", err)
	}

	if c.Interactive {
		stdinCtx, cancelStdin := context.WithCancel(ctx)
		defer cancelStdin()
		go c.readStdinLoop(stdinCtx, conn, cancelStdin)
	}

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

		c.updatePingInterval(msg)

		if msg.Type == "system" && msg.Event == "task_finished" {
			closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "task completed")
			_ = c.writeControl(conn, websocket.CloseMessage, closeMsg, time.Now().Add(c.writeTimeout))
			return ErrTaskFinished
		}

		if c.OnMessage != nil {
			c.OnMessage(msg)
		} else {
			c.defaultHandler(msg)
		}
	}
}

func (c *Client) updatePingInterval(msg models.ClientMessage) {
	if msg.Type != "system" {
		return
	}
	switch msg.Event {
	case "queue_position":
		c.setPingInterval(defaultpassivePingDuration)
	case "task_started":
		c.setPingInterval(defaultactivePingDuration)
	case "task_finished":
		c.setPingInterval(0)
	}
}

func (c *Client) setPingInterval(d time.Duration) {
	select {
	case c.pingIntervalCh <- d:
	default:
	}
}

// pingLoop отправляет ping каждые pingInterval
func (c *Client) pingLoop(ctx context.Context, conn *websocket.Conn) {
	var ticker *time.Ticker
	var tickerCh <-chan time.Time

	if c.pingInterval > 0 {
		ticker = time.NewTicker(c.pingInterval)
		tickerCh = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case newInterval := <-c.pingIntervalCh:
			//kill okd ticker
			if ticker != nil {
				ticker.Stop()
				ticker = nil
				tickerCh = nil
			}
			//start new
			if newInterval > 0 { //stop pings if newInterval is 0
				ticker = time.NewTicker(newInterval)
				tickerCh = ticker.C
			}
		case <-tickerCh:
			if err := c.writeControl(conn, websocket.PingMessage, nil, time.Now().Add(c.writeTimeout)); err != nil {
				c.Logger.Error("ping failed", "error", err)
				return
			}
		}
	}

}

// writeJSON выполняет запись JSON с таймаутом.
func (c *Client) writeJSON(conn *websocket.Conn, v any) error {
	conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return conn.WriteJSON(v)
}

// writeControl отправляет контрольное сообщение.
func (c *Client) writeControl(conn *websocket.Conn, messageType int, data []byte, deadline time.Time) error {
	conn.SetWriteDeadline(deadline)
	return conn.WriteMessage(messageType, data)
}

// readStdinLoop читает данные из Stdin и отправляет их как WSCommand input.
// При фатальной ошибке вызывает cancel, чтобы прервать Execute.
func (c *Client) readStdinLoop(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc) {
	buf := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := c.Stdin.Read(buf)
		if n > 0 {
			input := models.WSCommand{
				Type:  models.WSCmdInput,
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

// defaultHandler встроенная обработка сообщений, когда OnMessage == nil.
func (c *Client) defaultHandler(msg models.ClientMessage) {
	switch msg.Type {
	case "output":
		fmt.Fprint(c.Stdout, msg.Data)
		if msg.Delim != "" {
			fmt.Fprint(c.Stdout, msg.Delim)
		} else {
			if len(msg.Data) > 0 && msg.Data[len(msg.Data)-1] != '\n' {
				fmt.Fprintln(c.Stdout)
			}
		}
	case "system":
		switch msg.Event {
		case "queue_position":
			fmt.Fprintf(c.Stderr, "[queue] position: %d\r", msg.Pos)
		case "task_started":
			fmt.Fprintln(c.Stderr, "[queue] a task started executing")
		default:
			fmt.Fprintf(c.Stderr, "[system] event: %s\n", msg.Event)
		}
	default:
		fmt.Fprintf(c.Stderr, "[client] unknown message type: %s\n", msg.Type)
	}
}
