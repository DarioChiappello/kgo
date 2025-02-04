package kafkaclient

import (
	"github.com/DarioChiappello/kgo/internal"
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

func (c *Client) CreateTopic(topic string, partitions int32, replicationFactor int16) error {
	return c.conn.CreateTopic(topic, partitions, replicationFactor)
}

func (c *Client) Produce(topic string, key, value []byte) error {
	return c.conn.Produce(topic, key, value)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
