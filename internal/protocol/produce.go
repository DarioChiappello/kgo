package protocol

import (
	"bytes"
	"encoding/binary"
)

type ProduceRequest struct {
	Topic string
	Key   []byte
	Value []byte
}

func NewProduceRequest(topic string, key, value []byte) *ProduceRequest {
	return &ProduceRequest{
		Topic: topic,
		Key:   key,
		Value: value,
	}
}

func (r *ProduceRequest) ApiKey() int16     { return 0 } // Kafka Produce API key
func (r *ProduceRequest) ApiVersion() int16 { return 0 }

func (r *ProduceRequest) Encode() []byte {
	buf := new(bytes.Buffer)

	// Topic name
	binary.Write(buf, binary.BigEndian, int16(len(r.Topic)))
	buf.WriteString(r.Topic)

	// Partition (default to 0)
	binary.Write(buf, binary.BigEndian, int32(0))

	// Message set
	msgSet := encodeMessage(r.Key, r.Value)
	binary.Write(buf, binary.BigEndian, int32(len(msgSet)))
	buf.Write(msgSet)

	return buf.Bytes()
}

func encodeMessage(key, value []byte) []byte {
	buf := new(bytes.Buffer)

	// Offset
	binary.Write(buf, binary.BigEndian, int64(0))

	// Message size
	msgSize := 4 + 1 + 1 + 4 + len(key) + 4 + len(value)
	binary.Write(buf, binary.BigEndian, int32(msgSize))

	// CRC (placeholder)
	binary.Write(buf, binary.BigEndian, int32(0))

	// Magic byte
	buf.WriteByte(0)

	// Attributes
	buf.WriteByte(0)

	// Key
	binary.Write(buf, binary.BigEndian, int32(len(key)))
	buf.Write(key)

	// Value
	binary.Write(buf, binary.BigEndian, int32(len(value)))
	buf.Write(value)

	return buf.Bytes()
}

func (r *ProduceRequest) Size() int32 {
	return int32(2 + len(r.Topic) + 4 + 4 + len(encodeMessage(r.Key, r.Value)))
}
