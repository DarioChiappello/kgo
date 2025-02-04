package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

type Response interface {
	Decode([]byte) error
}

type ResponseHeader struct {
	CorrelationID int32
}

func DecodeHeader(data []byte) (ResponseHeader, error) {
	var header ResponseHeader
	buf := bytes.NewReader(data)

	// Kafka response header structure:
	// - Length (int32) - we've already read this
	// - CorrelationID (int32)
	if err := binary.Read(buf, binary.BigEndian, &header.CorrelationID); err != nil {
		return header, fmt.Errorf("error reading correlation ID: %v", err)
	}

	return header, nil
}

type MetadataResponse struct {
	Brokers []Broker
	Topics  []TopicMetadata
}

type Broker struct {
	ID   int32
	Host string
	Port int32
}

type TopicMetadata struct {
	TopicErrorCode int16
	TopicName      string
	Partitions     []PartitionMetadata
}

type PartitionMetadata struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

func (r *MetadataResponse) Decode(data []byte) error {
	buf := bytes.NewReader(data)

	// Read brokers
	var brokerCount int32
	if err := binary.Read(buf, binary.BigEndian, &brokerCount); err != nil {
		return err
	}

	r.Brokers = make([]Broker, brokerCount)
	for i := 0; i < int(brokerCount); i++ {
		if err := binary.Read(buf, binary.BigEndian, &r.Brokers[i].ID); err != nil {
			return err
		}

		var hostLen int16
		if err := binary.Read(buf, binary.BigEndian, &hostLen); err != nil {
			return err
		}
		hostBytes := make([]byte, hostLen)
		if _, err := buf.Read(hostBytes); err != nil {
			return err
		}
		r.Brokers[i].Host = string(hostBytes)

		if err := binary.Read(buf, binary.BigEndian, &r.Brokers[i].Port); err != nil {
			return err
		}
	}

	// Read topics
	var topicCount int32
	if err := binary.Read(buf, binary.BigEndian, &topicCount); err != nil {
		return err
	}

	r.Topics = make([]TopicMetadata, topicCount)
	for i := 0; i < int(topicCount); i++ {
		if err := binary.Read(buf, binary.BigEndian, &r.Topics[i].TopicErrorCode); err != nil {
			return err
		}

		var topicNameLen int16
		if err := binary.Read(buf, binary.BigEndian, &topicNameLen); err != nil {
			return err
		}
		topicNameBytes := make([]byte, topicNameLen)
		if _, err := buf.Read(topicNameBytes); err != nil {
			return err
		}
		r.Topics[i].TopicName = string(topicNameBytes)

		// Read partitions
		var partitionCount int32
		if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
			return err
		}

		r.Topics[i].Partitions = make([]PartitionMetadata, partitionCount)
		for j := 0; j < int(partitionCount); j++ {
			partition := &r.Topics[i].Partitions[j]
			if err := binary.Read(buf, binary.BigEndian, &partition.PartitionErrorCode); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.BigEndian, &partition.PartitionID); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.BigEndian, &partition.Leader); err != nil {
				return err
			}

			// Read replicas
			var replicaCount int32
			if err := binary.Read(buf, binary.BigEndian, &replicaCount); err != nil {
				return err
			}
			partition.Replicas = make([]int32, replicaCount)
			if err := binary.Read(buf, binary.BigEndian, &partition.Replicas); err != nil {
				return err
			}

			// Read ISR
			var isrCount int32
			if err := binary.Read(buf, binary.BigEndian, &isrCount); err != nil {
				return err
			}
			partition.Isr = make([]int32, isrCount)
			if err := binary.Read(buf, binary.BigEndian, &partition.Isr); err != nil {
				return err
			}
		}
	}

	return nil
}

type ProduceResponse struct {
	Topics []ProduceTopicResponse
}

type ProduceTopicResponse struct {
	TopicName  string
	Partitions []ProducePartitionResponse
}

type ProducePartitionResponse struct {
	PartitionID int32
	ErrorCode   int16
	Offset      int64
}

func (r *ProduceResponse) Decode(data []byte) error {
	buf := bytes.NewReader(data)

	var topicCount int32
	if err := binary.Read(buf, binary.BigEndian, &topicCount); err != nil {
		return err
	}

	r.Topics = make([]ProduceTopicResponse, topicCount)
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

		var partitionCount int32
		if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
			return err
		}

		r.Topics[i].Partitions = make([]ProducePartitionResponse, partitionCount)
		for j := 0; j < int(partitionCount); j++ {
			if err := binary.Read(buf, binary.BigEndian, &r.Topics[i].Partitions[j].PartitionID); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.BigEndian, &r.Topics[i].Partitions[j].ErrorCode); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.BigEndian, &r.Topics[i].Partitions[j].Offset); err != nil {
				return err
			}
		}
	}

	return nil
}

func ParseErrorCode(errorCode int16) error {
	switch errorCode {
	case 0:
		return nil
	case 3:
		return errors.New("unknown topic or partition")
	case 5:
		return errors.New("leader not available")
	case 7:
		return errors.New("request timed out")
	case 41:
		return errors.New("invalid replication factor")
	// Add more error codes as needed
	default:
		return fmt.Errorf("unknown error code: %d", errorCode)
	}
}
