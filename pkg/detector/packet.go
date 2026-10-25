package detector

import (
	"encoding/json"
	"fmt"
)

// HeartbeatPacket is the UDP datagram transmitted between primary and backup pods.
type HeartbeatPacket struct {
	MatchID   string `json:"match_id"`
	PodName   string `json:"pod_name"`
	Role      string `json:"role"`
	Epoch     int64  `json:"epoch"`
	Sequence  int64  `json:"sequence"`
	Timestamp int64  `json:"timestamp"`
}

func encodePacket(p HeartbeatPacket) ([]byte, error) {
	return json.Marshal(p)
}

func decodePacket(data []byte) (HeartbeatPacket, error) {
	var p HeartbeatPacket
	if err := json.Unmarshal(data, &p); err != nil {
		return HeartbeatPacket{}, fmt.Errorf("detector: decode packet: %w", err)
	}
	return p, nil
}
