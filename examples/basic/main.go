package main

import (
	"fmt"

	kafkaclient "github.com/DarioChiappello/kgo"
)

func main() {
	client, err := kafkaclient.NewClient([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create topic
	err = client.CreateTopic("test-topic", 3, 1)
	if err != nil {
		fmt.Println("Error creating topic:", err)
	}

	// Produce message
	err = client.Produce("test-topic", []byte("key"), []byte("value"))
	if err != nil {
		fmt.Println("Error producing message:", err)
	}
}
