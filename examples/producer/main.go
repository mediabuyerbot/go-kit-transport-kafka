package main

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
	kafkatransport "github.com/mediabuyerbot/go-kit-transport-kafka"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	brokers := []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	sayHi := kafkatransport.NewProducer(
		"response",
		client,
		encodeMsg,
		[]kafkatransport.ProducerOption{}...,
	).Endpoint()

	for i := 0; i < 100; i++ {
		ok, err := sayHi(ctx, "ho")
		log.Println(ok, err)
	}
}

func encodeMsg(ctx context.Context, request interface{}) (msg *sarama.ProducerMessage, err error) {
	return &sarama.ProducerMessage{
		Value: sarama.StringEncoder("HOHO"),
	}, nil
}
