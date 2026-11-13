package protocol

import "encoding/json"

// MessageType distinguishes incoming commands and outgoing notifications.
type MessageType string

const (
	TypeActionRequest   MessageType = "action_request"
	TypeActionResponse  MessageType = "action_response"
	TypeStateBroadcast  MessageType = "state_broadcast"
	TypePromotionNotice MessageType = "promotion_notice"
	TypeHeartbeat       MessageType = "heartbeat"
	TypeErrorResponse   MessageType = "error_response"
	TypeBotStep         MessageType = "bot_step"
	TypeNewGame         MessageType = "new_game"
)

// Envelope is the standard wire envelope for all IPC, RPC, and WebSocket communications.
type Envelope struct {
	Type      MessageType     `json:"type"`
	MatchID   string          `json:"match_id"`
	Sequence  int64           `json:"sequence,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// ErrorPayload describes errors returned to clients or proxy.
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
