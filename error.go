package mq

import "errors"

var (
	ErrInvalidTopic         = errors.New("invalid topic")
	ErrInvalidChannel       = errors.New("invalid channel")
	ErrMessageConsumerPanic = errors.New("message consumer panic")
)
