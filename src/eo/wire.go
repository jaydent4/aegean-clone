package eo

import (
	"encoding/base64"
	"fmt"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

func EncodeRaftMessage(message raftpb.Message) (map[string]any, error) {
	data, err := message.Marshal()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"type":         MessageTypeRaft,
		raftMessageKey: base64.StdEncoding.EncodeToString(data),
	}, nil
}

func DecodeRaftMessage(payload map[string]any) (raftpb.Message, error) {
	encoded, ok := payload[raftMessageKey].(string)
	if !ok || encoded == "" {
		return raftpb.Message{}, fmt.Errorf("missing %q", raftMessageKey)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return raftpb.Message{}, err
	}

	var message raftpb.Message
	if err := message.Unmarshal(data); err != nil {
		return raftpb.Message{}, err
	}
	return message, nil
}
