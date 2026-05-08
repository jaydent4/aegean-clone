package pbeo

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

func EncodeRaftMessages(messages []raftpb.Message) (map[string]any, error) {
	if len(messages) == 1 {
		return EncodeRaftMessage(messages[0])
	}
	encoded := make([]string, 0, len(messages))
	for _, message := range messages {
		data, err := message.Marshal()
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, base64.StdEncoding.EncodeToString(data))
	}
	return map[string]any{
		"type":          MessageTypeRaft,
		raftMessagesKey: encoded,
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

func DecodeRaftMessages(payload map[string]any) ([]raftpb.Message, error) {
	if _, ok := payload[raftMessageKey]; ok {
		message, err := DecodeRaftMessage(payload)
		if err != nil {
			return nil, err
		}
		return []raftpb.Message{message}, nil
	}
	rawMessages, ok := payload[raftMessagesKey]
	if !ok {
		return nil, fmt.Errorf("missing %q", raftMessagesKey)
	}
	var encoded []string
	switch values := rawMessages.(type) {
	case []string:
		encoded = values
	case []any:
		encoded = make([]string, 0, len(values))
		for _, value := range values {
			text, ok := value.(string)
			if !ok || text == "" {
				return nil, fmt.Errorf("invalid %q entry", raftMessagesKey)
			}
			encoded = append(encoded, text)
		}
	default:
		return nil, fmt.Errorf("invalid %q", raftMessagesKey)
	}
	messages := make([]raftpb.Message, 0, len(encoded))
	for _, item := range encoded {
		data, err := base64.StdEncoding.DecodeString(item)
		if err != nil {
			return nil, err
		}
		var message raftpb.Message
		if err := message.Unmarshal(data); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}
