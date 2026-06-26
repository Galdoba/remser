package client

import (
	"io"
	"log/slog"
	"time"

	"github.com/Galdoba/remser/internal/infrastructure/config"
	"github.com/gorilla/websocket"
)

// Option is a functional option for constructing a Client.
type Option func(*Client)

// WithStdin sets the io.Reader for interactive input.
func WithStdin(r io.Reader) Option {
	return func(c *Client) { c.Stdin = r }
}

// WithStdout overrides the stdout writer.
func WithStdout(w io.Writer) Option {
	return func(c *Client) { c.Stdout = w }
}

// WithStderr overrides the stderr writer.
func WithStderr(w io.Writer) Option {
	return func(c *Client) { c.Stderr = w }
}

// WithLogger sets the structured logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.Logger = l }
}

// WithDialer allows using a custom websocket.Dialer.
func WithDialer(d *websocket.Dialer) Option {
	return func(c *Client) { c.Dialer = d }
}

// WithClientID sets the client identifier sent to the server.
func WithClientID(id string) Option {
	return func(c *Client) { c.ClientID = id }
}

// WithInteractive enables interactive mode (stdin forwarding).
func WithInteractive(interactive bool) Option {
	return func(c *Client) { c.Interactive = interactive }
}

// WithPingInterval sets the interval between client‑initiated ping frames.
// A value of 0 disables client pings, but the client still responds to
// server pings.
func WithPingInterval(d time.Duration) Option {
	return func(c *Client) { c.pingInterval = d }
}

// WithWriteTimeout sets the deadline for writing a message.
func WithWriteTimeout(d time.Duration) Option {
	return func(c *Client) { c.writeTimeout = d }
}

// WithPongTimeout sets the maximum time to wait for a pong response from the server.
func WithPongTimeout(d time.Duration) Option {
	return func(c *Client) { c.pongTimeout = d }
}

// WithSSHTunnel configures the client to connect through an SSH tunnel.
// The cfg must contain at least one authentication method (password or key).
func WithSSHTunnel(cfg config.SSH) Option {
	return func(c *Client) {
		c.useSSH = true
		c.sshCfg = cfg
	}
}
