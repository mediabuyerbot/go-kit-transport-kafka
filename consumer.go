package kafkatransport

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

type ConsumerHandler func(ctx context.Context, message interface{}) error

type Consumer struct {
	topic    string
	client   sarama.ConsumerGroup
	handlers []ConsumerHandler
	dec      DecodeMessageFunc
}

func (c *Consumer) HandleFunc(handler ConsumerHandler) {
	c.handlers = append(c.handlers, handler)
}

func (c *Consumer) Consume(ctx context.Context) {
	groupHandler := newNativeHandler(c.handlers, c.dec)
	go func() {
		for {
			if err := c.client.Consume(ctx, []string{c.topic}, groupHandler); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	<-groupHandler.ready
}

func NewConsumer(
	channel string,
	client sarama.ConsumerGroup,
	dec DecodeMessageFunc) *Consumer {
	consumer := &Consumer{
		dec:      dec,
		topic:    channel,
		client:   client,
		handlers: []ConsumerHandler{},
	}
	return consumer
}

type nativeHandler struct {
	dec      DecodeMessageFunc
	ready    chan struct{}
	handlers []ConsumerHandler
}

func newNativeHandler(
	handlers []ConsumerHandler,
	decoder DecodeMessageFunc,
) *nativeHandler {
	return &nativeHandler{
		dec:      decoder,
		handlers: handlers,
		ready:    make(chan struct{}),
	}
}

func (h *nativeHandler) Setup(s sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *nativeHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *nativeHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for message := range claim.Messages() {
		request, err := h.dec(ctx, message)
		if err != nil {
			return err
		}
		for _, handler := range h.handlers {
			if err := handler(ctx, request); err != nil {
				return err
			}
		}
	}
	return nil
}
