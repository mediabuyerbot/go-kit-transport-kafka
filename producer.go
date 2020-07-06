package kafkatransport

import (
	"context"

	"github.com/Shopify/sarama"

	"github.com/go-kit/kit/endpoint"
)

type (
	ProducerRequestFunc  func(context.Context) context.Context
	ProducerResponseFunc func(partition int32, offset int64, msg *sarama.ProducerMessage)
	ProducerOption       func(*Producer)
)

type Producer struct {
	topic  string
	client sarama.Client
	syncP  sarama.SyncProducer
	enc    EncodeMessageFunc
	before []ProducerRequestFunc
	after  []ProducerResponseFunc
}

func NewProducer(
	topic string,
	client sarama.Client,
	enc EncodeMessageFunc,
	options ...ProducerOption,
) *Producer {
	p := &Producer{
		enc:    enc,
		client: client,
		topic:  topic,
		before: []ProducerRequestFunc{},
		after:  []ProducerResponseFunc{},
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}
	p.syncP = producer
	for _, option := range options {
		option(p)
	}
	return p
}

func (p Producer) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		for _, f := range p.before {
			ctx = f(ctx)
		}
		msg, err := p.enc(ctx, request)
		if err != nil {
			return nil, err
		}
		msg.Topic = p.topic
		partition, offset, err := p.syncP.SendMessage(msg)
		if err != nil {
			return false, err
		}
		for _, f := range p.after {
			f(partition, offset, msg)
		}
		return true, nil
	}
}

func ProducerBefore(before ...ProducerRequestFunc) ProducerOption {
	return func(c *Producer) { c.before = append(c.before, before...) }
}

func ProducerAfter(after ...ProducerResponseFunc) ProducerOption {
	return func(c *Producer) { c.after = append(c.after, after...) }
}
