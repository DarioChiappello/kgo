package protocol

import (
	"bytes"
	"encoding/binary"
)

// CreateTopicsRequest (API Key 19)
type CreateTopicsRequest struct {
	Topic             string
	Partitions        int32
	ReplicationFactor int16
	Timeout           int32
}

func NewCreateTopicsRequest(topic string, partitions int32, replicationFactor int16) *CreateTopicsRequest {
	return &CreateTopicsRequest{
		Topic:             topic,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		Timeout:           30000, // 30 seconds default
	}
}

func (r *CreateTopicsRequest) ApiKey() int16     { return 19 }
func (r *CreateTopicsRequest) ApiVersion() int16 { return 0 }

func (r *CreateTopicsRequest) Encode() []byte {
	buf := new(bytes.Buffer)

	// Topic
	binary.Write(buf, binary.BigEndian, int16(len(r.Topic)))
	buf.WriteString(r.Topic)

	// Partitions
	binary.Write(buf, binary.BigEndian, r.Partitions)

	// ReplicationFactor
	binary.Write(buf, binary.BigEndian, r.ReplicationFactor)

	// ReplicaAssignment (empty)
	binary.Write(buf, binary.BigEndian, int32(0))

	// Configurations (empty)
	binary.Write(buf, binary.BigEndian, int32(0))

	// Timeout
	binary.Write(buf, binary.BigEndian, r.Timeout)

	return buf.Bytes()
}

func (r *CreateTopicsRequest) Size() int32 {
	return int32(2 + len(r.Topic) + 4 + 2 + 4 + 4 + 4)
}

type CreateTopicsResponse struct {
	Topics []TopicCreationResult
}

type TopicCreationResult struct {
	TopicName    string
	ErrorCode    int16
	ErrorMessage string
}

func (r *CreateTopicsResponse) Decode(data []byte) error {
	buf := bytes.NewReader(data)

	var topicCount int32
	if err := binary.Read(buf, binary.BigEndian, &topicCount); err != nil {
		return err
	}

	r.Topics = make([]TopicCreationResult, topicCount)
	for i := 0; i < int(topicCount); i++ {
		var topicNameLen int16
		if err := binary.Read(buf, binary.BigEndian, &topicNameLen); err != nil {
			return err
		}
		topicNameBytes := make([]byte, topicNameLen)
		if _, err := buf.Read(topicNameBytes); err != nil {
			return err
		}
		r.Topics[i].TopicName = string(topicNameBytes)

		if err := binary.Read(buf, binary.BigEndian, &r.Topics[i].ErrorCode); err != nil {
			return err
		}

		var errorMessageLen int16
		if err := binary.Read(buf, binary.BigEndian, &errorMessageLen); err != nil {
			return err
		}
		errorMessageBytes := make([]byte, errorMessageLen)
		if _, err := buf.Read(errorMessageBytes); err != nil {
			return err
		}
		r.Topics[i].ErrorMessage = string(errorMessageBytes)
	}

	return nil
}
