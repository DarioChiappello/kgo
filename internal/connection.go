package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/DarioChiappello/kgo/internal/protocol"
)

type Connection struct {
	conn        net.Conn
	broker      string
	correlation int32
}

func NewConnection(broker string) (*Connection, error) {
	conn, err := net.DialTimeout("tcp", broker, 5*time.Second)
	if err != nil {
		return nil, err
	}

	return &Connection{
		conn:   conn,
		broker: broker,
	}, nil
}

func (c *Connection) CreateTopic(topic string, partitions int32, replicationFactor int16) error {
	req := protocol.NewMetadataRequest(topic, partitions, replicationFactor)
	_, err := c.sendRequest(req)
	return err
}

func (c *Connection) Produce(topic string, key, value []byte) error {
	req := protocol.NewProduceRequest(topic, key, value)
	_, err := c.sendRequest(req)
	return err
}

// func (c *Connection) sendRequest(req protocol.Request) ([]byte, error) {
// 	c.correlation++

// 	buf := new(bytes.Buffer)
// 	binary.Write(buf, binary.BigEndian, req.Size())
// 	binary.Write(buf, binary.BigEndian, req.ApiKey())
// 	binary.Write(buf, binary.BigEndian, req.ApiVersion())
// 	binary.Write(buf, binary.BigEndian, c.correlation)
// 	binary.Write(buf, binary.BigEndian, req.Encode())

// 	_, err := c.conn.Write(buf.Bytes())
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Read response (simplified)
// 	header := make([]byte, 8)
// 	_, err = c.conn.Read(header)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return header, nil
// }

func (c *Connection) sendRequest(req protocol.Request) (protocol.Response, error) {
	c.correlation++

	// Send request
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, req.Size())
	binary.Write(buf, binary.BigEndian, req.ApiKey())
	binary.Write(buf, binary.BigEndian, req.ApiVersion())
	binary.Write(buf, binary.BigEndian, c.correlation)
	binary.Write(buf, binary.BigEndian, req.Encode())

	_, err := c.conn.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	// Read response
	header := make([]byte, 4) // Read length prefix
	_, err = io.ReadFull(c.conn, header)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	data := make([]byte, length)
	_, err = io.ReadFull(c.conn, data)
	if err != nil {
		return nil, err
	}

	// Parse response based on request type
	switch req.(type) {
	case *protocol.MetadataRequest:
		resp := &protocol.MetadataResponse{}
		if err := resp.Decode(data); err != nil {
			return nil, err
		}
		return resp, nil
	case *protocol.ProduceRequest:
		resp := &protocol.ProduceResponse{}
		if err := resp.Decode(data); err != nil {
			return nil, err
		}
		return resp, nil
	default:
		return nil, errors.New("unknown request type")
	}
}

func (c *Connection) Close() error {
	return c.conn.Close()
}
