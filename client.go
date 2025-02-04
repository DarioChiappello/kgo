package kafkaclient

import (
	"errors"

	"github.com/DarioChiappello/kgo/internal"
	"github.com/DarioChiappello/kgo/internal/protocol"
)

type Client struct {
	conn *internal.Connection
}

func NewClient(brokers []string) (*Client, error) {
	// For simplicity, connect to first broker
	conn, err := internal.NewConnection(brokers[0])
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// func (c *Client) CreateTopic(topic string, partitions int32, replicationFactor int16) error {
// 	return c.conn.CreateTopic(topic, partitions, replicationFactor)
// }

// Updated CreateTopic method
// func (c *Client) CreateTopic(topic string, partitions int32, replicationFactor int16) error {
// 	resp, err := c.conn.sendRequest(protocol.NewMetadataRequest(topic, partitions, replicationFactor))
// 	if err != nil {
// 		return err
// 	}

// 	metadataResp, ok := resp.(*protocol.MetadataResponse)
// 	if !ok {
// 		return errors.New("unexpected response type")
// 	}

// 	// Check for errors in the response
// 	for _, t := range metadataResp.Topics {
// 		if t.TopicErrorCode != 0 {
// 			return protocol.ParseErrorCode(t.TopicErrorCode)
// 		}
// 	}

// 	return nil
// }

func (c *Client) CreateTopic(topic string, partitions int32, replicationFactor int16) error {
	resp, err := c.conn.SendRequest(protocol.NewMetadataRequest(topic, partitions, replicationFactor))
	if err != nil {
		return err
	}

	metadataResp, ok := resp.(*protocol.MetadataResponse)
	if !ok {
		return errors.New("unexpected response type")
	}

	// Check for errors in the response
	for _, t := range metadataResp.Topics {
		if t.TopicErrorCode != 0 {
			return protocol.ParseErrorCode(t.TopicErrorCode)
		}
	}

	return nil
}

func (c *Client) Produce(topic string, key, value []byte) error {
	return c.conn.Produce(topic, key, value)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
