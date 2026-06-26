package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"
)

// setupSSHTunnelIfNeeded returns a copy of the client's dialer. If SSH is
// enabled, it establishes a connection to the SSH server and modifies the
// dialer's NetDialContext so that all WebSocket traffic is routed through
// the tunnel. The caller is responsible for closing the returned ssh.Client.
func (c *Client) setupSSHTunnelIfNeeded() (*websocket.Dialer, *ssh.Client, error) {
	dialer := *c.Dialer // copy to avoid mutating the original

	if !c.useSSH {
		return &dialer, nil, nil
	}

	authMethods, err := c.sshAuthMethods()
	if err != nil {
		return nil, nil, err
	}

	hostKeyCB, err := c.hostKeyCallback()
	if err != nil {
		return nil, nil, err
	}

	port := c.sshCfg.Port
	if port == "" {
		port = defaultSSHPort
	}
	sshAddr := net.JoinHostPort(c.sshCfg.Host, port)

	sshCfg := &ssh.ClientConfig{
		User:            c.sshCfg.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCB,
		Timeout:         10 * time.Second,
	}

	sshClient, err := ssh.Dial("tcp", sshAddr, sshCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh dial: %w", err)
	}

	dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		target := c.sshCfg.RemoteAddress
		if target == "" {
			target = addr
		}
		c.Logger.Debug("ssh tunnel dial", "target", target)
		return sshClient.DialContext(ctx, network, target)
	}

	return &dialer, sshClient, nil
}

// sshAuthMethods builds the list of SSH authentication methods from config.
func (c *Client) sshAuthMethods() ([]ssh.AuthMethod, error) {
	methods := make([]ssh.AuthMethod, 0)
	if c.sshCfg.Password != "" {
		methods = append(methods, ssh.Password(c.sshCfg.Password))
	}
	if c.sshCfg.KeyFile != "" {
		key, err := os.ReadFile(c.sshCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("reading key file: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("parsing key: %w", err)
		}
		methods = append(methods, ssh.PublicKeys(signer))
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("at least one SSH auth method required (password or key)")
	}
	return methods, nil
}

// hostKeyCallback returns the appropriate SSH host key verification callback.
func (c *Client) hostKeyCallback() (ssh.HostKeyCallback, error) {
	if c.sshCfg.InsecureIgnoreHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	// In production, a proper known_hosts verification would be implemented here.
	return nil, fmt.Errorf("host key verification is not implemented; use --ssh-insecure to skip")
}
