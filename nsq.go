package mq

import (
	"context"

	"github.com/nsqio/go-nsq"
	"github.com/nzai/log"
)

type NSQConfig struct {
	NsqdAddress         string
	ConnectToLookupd    bool
	NsqLookupdAddresses []string
	TopicPrefix         string
	*nsq.Config
}

type NSQ struct {
	ctx        context.Context
	config     *NSQConfig
	cancelFunc context.CancelFunc
	producer   *nsq.Producer
}

func NewNSQ(ctx context.Context, config *NSQConfig) (*NSQ, error) {
	ctx, cancelFunc := context.WithCancel(ctx)

	q := &NSQ{
		ctx:        ctx,
		config:     config,
		cancelFunc: cancelFunc,
	}

	var err error
	q.producer, err = nsq.NewProducer(config.NsqdAddress, config.Config)
	if err != nil {
		log.Warn(ctx, "create new producer failed", log.Err(err), log.Any("config", config))
		return nil, err
	}

	err = q.producer.Ping()
	if err != nil {
		log.Warn(ctx, "ping producer failed", log.Err(err), log.Any("config", config))
		return nil, err
	}

	q.producer.SetLogger(q, nsq.LogLevelError)

	return q, nil
}

func (s NSQ) Publish(ctx context.Context, topic string, body []byte) error {
	topic = s.config.TopicPrefix + topic
	valid := nsq.IsValidTopicName(topic)
	if !valid {
		log.Warn(ctx, "invalid topic name", log.String("topic", topic))
		return ErrInvalidTopic
	}

	err := s.producer.Publish(topic, body)
	if err != nil {
		log.Warn(ctx, "publish message failed",
			log.Err(err),
			log.String("topic", topic),
			log.ByteString("body", body))
		return err
	}

	log.Debug(ctx, "publish message successfully", log.String("topic", topic), log.ByteString("body", body))

	return nil
}

func (s NSQ) Consume(ctx context.Context, topic, channel string, handler func(ctx context.Context, body []byte) error) error {
	topic = s.config.TopicPrefix + topic
	valid := nsq.IsValidTopicName(topic)
	if !valid {
		log.Warn(ctx, "invalid topic name", log.String("topic", topic))
		return ErrInvalidTopic
	}

	valid = nsq.IsValidChannelName(channel)
	if !valid {
		log.Warn(ctx, "invalid channel name", log.String("channel", channel))
		return ErrInvalidChannel
	}

	consumer, err := nsq.NewConsumer(topic, channel, s.config.Config)
	if err != nil {
		log.Warn(ctx, "create new customer failed", log.Err(err), log.Any("config", s.config))
		return err
	}

	consumer.SetLoggerLevel(nsq.LogLevelError)
	consumer.SetLogger(s, nsq.LogLevelError)

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) (err error) {
		defer func() {
			err1 := recover()
			if err1 == nil {
				return
			}

			log.Warn(ctx, "message consumer panic", log.Any("err", err1))
			err = ErrMessageConsumerPanic
		}()

		err = handler(ctx, message.Body)
		if err != nil {
			log.Warn(ctx, "handle message failed", log.Err(err), log.ByteString("body", message.Body))
			return err
		}

		log.Debug(ctx, "consume message successfully",
			log.String("topic", topic),
			log.String("channel", channel),
			log.ByteString("body", message.Body))

		return nil
	}))

	if s.config.ConnectToLookupd {
		err = consumer.ConnectToNSQLookupds(s.config.NsqLookupdAddresses)
		if err != nil {
			log.Warn(ctx, "connect to nsqlookupd failed", log.Err(err), log.Any("config", s.config))
			return err
		}
	} else {
		err = consumer.ConnectToNSQD(s.config.NsqdAddress)
		if err != nil {
			log.Warn(ctx, "connect to nsqd failed", log.Err(err), log.Any("config", s.config))
			return err
		}
	}

	go func() {
		defer consumer.Stop()

		<-ctx.Done()
		log.Debug(ctx, "consume stopped", log.String("topic", topic), log.String("channel", channel))

		if s.config.ConnectToLookupd {
			for _, address := range s.config.NsqLookupdAddresses {
				err = consumer.DisconnectFromNSQLookupd(address)
				if err != nil {
					log.Warn(ctx, "disconnect from nsqlookupd failed", log.Err(err), log.String("address", address))
				}
			}
		} else {
			err = consumer.DisconnectFromNSQD(s.config.NsqdAddress)
			if err != nil {
				log.Warn(ctx, "disconnect from nsqd failed", log.Err(err), log.String("address", s.config.NsqdAddress))
			}
		}
	}()

	return nil
}

func (s NSQ) Output(calldepth int, message string) error {
	log.Warn(s.ctx, message, log.Int("calldepth", calldepth), log.String("source", "nsq"))
	return nil
}

func (s NSQ) Close() error {
	if s.producer != nil {
		s.producer.Stop()
	}

	s.cancelFunc()

	return nil
}
