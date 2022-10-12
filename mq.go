package mq

import "context"

type MessageQueue interface {
	Publish(ctx context.Context, topic string, body []byte) error
	Consume(ctx context.Context, topic, channel string, handler func(ctx context.Context, body []byte) error) error
	Close() error
}
