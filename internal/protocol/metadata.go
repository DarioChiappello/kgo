package protocol

import (
	"bytes"
	"encoding/binary"
)

type MetadataRequest struct {
	Topic             string
	Partitions        int32
	ReplicationFactor int16
}

func NewMetadataRequest(topic string, partitions int32, replicationFactor int16) *MetadataRequest {
	return &MetadataRequest{
		Topic:             topic,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
	}
}

func (r *MetadataRequest) ApiKey() int16     { return 3 } // Kafka Metadata API key
func (r *MetadataRequest) ApiVersion() int16 { return 0 }

func (r *MetadataRequest) Encode() []byte {
	buf := new(bytes.Buffer)

	// Topic name
	binary.Write(buf, binary.BigEndian, int16(len(r.Topic)))
	buf.WriteString(r.Topic)

	// Partitions
	binary.Write(buf, binary.BigEndian, r.Partitions)

	// Replication factor
	binary.Write(buf, binary.BigEndian, r.ReplicationFactor)

	return buf.Bytes()
}

func (r *MetadataRequest) Size() int32 {
	return int32(2 + len(r.Topic) + 4 + 2)
}
