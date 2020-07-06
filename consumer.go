package kafkatransport

import (
	"context"

	"github.com/go-kit/kit/log"

	"github.com/go-kit/kit/transport"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
)

type (
	ConsumerOption      func(*Consumer)
	ConsumerHook        func(sarama.ConsumerGroupSession) error
	ConsumerRequestFunc func(context.Context) context.Context
)

type Consumer struct {
	e              endpoint.Endpoint
	topic          string
	client         sarama.ConsumerGroup
	dec            DecodeMessageFunc
	errorHandler   transport.ErrorHandler
	setupHandler   ConsumerHook
	cleanupHandler ConsumerHook
	before         []ConsumerRequestFunc
}

func (c *Consumer) Consume(ctx context.Context) {
	groupHandler := newNativeHandler(
		c.e,
		c.dec,
		c.errorHandler,
		c.setupHandler,
		c.cleanupHandler,
		c.before,
	)
	go func() {
		for {
			if err := c.client.Consume(ctx, []string{c.topic}, groupHandler); err != nil {
				c.errorHandler.Handle(ctx, err)
			}
			if ctx.Err() != nil {
				return
			}
			groupHandler.ready = make(chan struct{})
		}
	}()
	<-groupHandler.ready
}

func WithErrorHandler(errorHandler transport.ErrorHandler) ConsumerOption {
	return func(c *Consumer) { c.errorHandler = errorHandler }
}

func WithSetupHook(hook ConsumerHook) ConsumerOption {
	return func(c *Consumer) { c.setupHandler = hook }
}

func WithCleanupHook(hook ConsumerHook) ConsumerOption {
	return func(c *Consumer) { c.cleanupHandler = hook }
}

func ConsumerBefore(before ...ConsumerRequestFunc) ConsumerOption {
	return func(c *Consumer) { c.before = append(c.before, before...) }
}

func NewConsumer(
	channel string,
	endpoint endpoint.Endpoint,
	client sarama.ConsumerGroup,
	dec DecodeMessageFunc,
	options ...ConsumerOption) *Consumer {
	consumer := &Consumer{
		e:              endpoint,
		dec:            dec,
		topic:          channel,
		client:         client,
		setupHandler:   defaultHookHandler,
		cleanupHandler: defaultHookHandler,
		errorHandler:   transport.NewLogErrorHandler(log.NewNopLogger()),
		before:         []ConsumerRequestFunc{},
	}
	for _, option := range options {
		option(consumer)
	}
	return consumer
}

var defaultHookHandler = func(s sarama.ConsumerGroupSession) error { return nil }

type nativeHandler struct {
	dec          DecodeMessageFunc
	ready        chan struct{}
	e            endpoint.Endpoint
	errorHandler transport.ErrorHandler
	setupHook    ConsumerHook
	cleanupHook  ConsumerHook
	inject       []ConsumerRequestFunc
}

func newNativeHandler(
	e endpoint.Endpoint,
	decoder DecodeMessageFunc,
	errorHandler transport.ErrorHandler,
	setupHook ConsumerHook,
	cleanupHook ConsumerHook,
	inject []ConsumerRequestFunc,
) *nativeHandler {
	return &nativeHandler{
		setupHook:    setupHook,
		cleanupHook:  cleanupHook,
		errorHandler: errorHandler,
		dec:          decoder,
		inject:       inject,
		e:            e,
		ready:        make(chan struct{}),
	}
}

func (h *nativeHandler) Setup(s sarama.ConsumerGroupSession) error {
	close(h.ready)
	return h.setupHook(s)
}

func (h *nativeHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return h.cleanupHook(s)
}

func (h *nativeHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for message := range claim.Messages() {
		for _, f := range h.inject {
			ctx = f(ctx)
		}

		request, err := h.dec(ctx, message)
		if err != nil {
			h.errorHandler.Handle(ctx, err)
			return err
		}

		response, err := h.e(ctx, request)
		if err != nil {
			h.errorHandler.Handle(ctx, err)
			return err
		}
		if e, ok := response.(error); ok && len(e.Error()) > 0 {
			h.errorHandler.Handle(ctx, e)
			continue
		}
		session.MarkMessage(message, "")
	}
	return nil
}
