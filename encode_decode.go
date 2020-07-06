package kafkatransport

import (
	"context"

	"github.com/Shopify/sarama"
)

type DecodeMessageFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (request interface{}, err error)
