package handler

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Galdoba/remser/api/models"
	"github.com/Galdoba/remser/internal/server/queue"
	"github.com/google/uuid"
)

// Handshaker performs the initial protocol negotiation over a WebSocket.
type Handshaker interface {
	Handshake(sess *Session, r *http.Request) (clientID string, extra interface{}, err error)
}

// InteractiveHandshake expects the first message to be of type "execute".
type InteractiveHandshake struct {
	deadline time.Duration
	log      *slog.Logger
}

func NewInteractiveHandshake(deadline time.Duration, log *slog.Logger) *InteractiveHandshake {
	return &InteractiveHandshake{deadline: deadline, log: log}
}

func (h *InteractiveHandshake) Handshake(sess *Session, r *http.Request) (string, interface{}, error) {
	sess.conn.SetReadDeadline(time.Now().Add(h.deadline))
	var first models.WSCommand
	if err := sess.conn.ReadJSON(&first); err != nil {
		h.log.Warn("first message deadline reached", "ip", sess.conn.RemoteAddr().String())
		return "", nil, err
	}
	sess.conn.SetReadDeadline(time.Time{})

	h.log.Info("received first message", "type", first.Type, "id", first.ClientID,
		"interactive", first.Interactive, "args", first.Args)

	if first.Type != models.WSCmdExecute {
		sess.WriteJSON(models.ClientMessage{
			Type:  msgTypeSystem,
			Event: msgEventError,
			Data:  "first message must be type=execute",
		})
		return "", nil, errors.New("invalid first message type")
	}

	clientID := first.ClientID
	if clientID == "" {
		clientID = queue.ExtractClientIP(r)
		h.log.Warn("no clientID provided, using IP", "ip", clientID)
	}
	clientID += "-" + uuid.New().String()[:8]

	return clientID, &first, nil
}