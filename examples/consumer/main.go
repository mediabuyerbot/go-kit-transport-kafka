package main

import (
	"context"
	"log"
	"time"

	kafkatransport "github.com/mediabuyerbot/go-kit-transport-kafka"

	"github.com/Shopify/sarama"

	"github.com/go-kit/kit/endpoint"
)

type Service interface {
	OpenAccount(ctx context.Context, a int, b int) error
}

type service struct {
}

func (service) OpenAccount(ctx context.Context, a int, b int) error {
	log.Println("open account", a, b)
	return nil
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	brokers := []string{"127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"}
	client, err := sarama.NewConsumerGroup(brokers, "demo", config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	svc := service{}

	kafkatransport.NewConsumer(
		"response",
		makeEndpoint(svc),
		client,
		decodeMessage,
		[]kafkatransport.ConsumerOption{
			kafkatransport.WithCleanupHook(func(session sarama.ConsumerGroupSession) error {
				log.Println("cleanup hook")
				return nil
			}),
			kafkatransport.WithSetupHook(func(session sarama.ConsumerGroupSession) error {
				log.Println("setup hook")
				return nil
			}),
		}...,
	).Consume(ctx)

	<-time.After(60 * time.Second)
	cancel()
}

func makeEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(OpenAccountRequest)
		err = s.OpenAccount(ctx, req.A, req.B)
		return GeneralResponse{Err: err}, nil
	}
}

func decodeMessage(ctx context.Context, msg *sarama.ConsumerMessage) (request interface{}, err error) {
	return OpenAccountRequest{A: 1, B: 2}, nil
}

type OpenAccountRequest struct {
	A int
	B int
}

type GeneralResponse struct {
	Err error
}

func (r GeneralResponse) Error() string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return ""
}
