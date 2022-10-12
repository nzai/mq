package mq

import (
	"context"

	"github.com/streadway/amqp"
	"github.com/nzai/log"
)

type RabbitMQConfig struct {
	URL          string `json:"url"`
	Exchange     string `json:"exchange"`
	ExchangeType string `json:"exchange_type"`
}

type RabbitMQ struct {
	config            *RabbitMQConfig
	publishConnection *amqp.Connection
	publishChannel    *amqp.Channel
	consumeConnection *amqp.Connection
	consumeChannel    *amqp.Channel
}

func NewRabbitMQ(ctx context.Context, config *RabbitMQConfig) (*RabbitMQ, error) {
	mq := &RabbitMQ{config: config}

	var err error
	mq.publishConnection, mq.publishChannel, err = mq.connect(ctx)
	if err != nil {
		return nil, err
	}

	mq.consumeConnection, mq.consumeChannel, err = mq.connect(ctx)
	if err != nil {
		return nil, err
	}

	return mq, nil
}

func (s RabbitMQ) connect(ctx context.Context) (*amqp.Connection, *amqp.Channel, error) {
	connection, err := amqp.Dial(s.config.URL)
	if err != nil {
		log.Error(ctx, "dail rabbitmq failed",
			log.Err(err),
			log.String("url", s.config.URL))
		return nil, nil, err
	}

	log.Debug(ctx, "connect to rabbitmq successfully", log.String("url", s.config.URL))

	channel, err := connection.Channel()
	if err != nil {
		log.Error(ctx, "get rabbitmq channel failed",
			log.Err(err),
			log.String("url", s.config.URL))
		return nil, nil, err
	}

	log.Debug(ctx, "get channel successfully", log.String("url", s.config.URL))

	err = channel.ExchangeDeclare(
		s.config.Exchange,     // name
		s.config.ExchangeType, // type: direct
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // noWait
		nil,                   // arguments
	)
	if err != nil {
		log.Error(ctx, "declare rabbitmq exchange failed",
			log.Err(err),
			log.Any("config", s.config))
		return nil, nil, err
	}

	log.Debug(ctx, "declare exchange successfully",
		log.String("exchangeName", s.config.Exchange),
		log.String("exchangeType", s.config.ExchangeType))

	return connection, channel, nil
}

func (s RabbitMQ) Publish(ctx context.Context, routeKey string, body []byte) error {
	err := s.publishChannel.Publish(
		s.config.Exchange, // name
		routeKey,          // routing to 0 or more queues
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			// MessageId:       messageID,
			Body:         body,
			DeliveryMode: amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:     0,               // 0-9
		},
	)
	if err != nil {
		log.Error(ctx, "publish message failed", log.Err(err), log.String("routeKey", routeKey))
		return err
	}

	log.Debug(ctx, "publish message successfully", log.String("routeKey", routeKey))

	return nil
}

func (s *RabbitMQ) Consume(ctx context.Context, routeKey, channel string, handler func(ctx context.Context, body []byte) error) error {
	queue, err := s.consumeChannel.QueueDeclare(
		channel, // name of the queue
		false,   // durable
		true,    // delete when unused
		true,    // exclusive
		false,   // noWait
		nil,     // arguments
	)
	if err != nil {
		log.Error(ctx, "declare queue failed", log.Err(err), log.String("routeKey", routeKey))
		return err
	}

	log.Info(ctx, "declare queue successfully", log.String("routeKey", routeKey))

	err = s.consumeChannel.QueueBind(
		queue.Name,        // name of the queue
		routeKey,          // bindingKey
		s.config.Exchange, // sourceExchange
		false,             // noWait
		nil,               // arguments
	)
	if err != nil {
		log.Error(ctx, "bind queue failed", log.Err(err), log.String("routeKey", routeKey))
		return err
	}

	log.Info(ctx, "bind queue successfully", log.String("routeKey", routeKey))

	deliveries, err := s.consumeChannel.Consume(
		queue.Name, // name
		"",         // consumerTag,
		false,      // noAck
		true,       // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		log.Error(ctx, "start consume failed",
			log.Err(err),
			log.String("routeKey", routeKey),
			log.String("queueName", queue.Name))
		return err
	}

	log.Info(ctx, "start consume successfully", log.String("routeKey", routeKey), log.String("queue", queue.Name))

	go func() {
		for delivery := range deliveries {
			err = handler(ctx, delivery.Body)
			if err != nil {
				log.Warn(ctx, "handle message failed", log.Err(err), log.Any("delivery", delivery))
				continue
			}

			err = delivery.Ack(true)
			if err != nil {
				log.Warn(ctx, "ack message failed",
					log.Err(err),
					log.String("routeKey", routeKey),
					log.Any("delivery", delivery))
				continue
			}

			log.Debug(ctx, "handle message successfully", log.String("routeKey", routeKey), log.String("messageID", delivery.MessageId))
		}

		log.Debug(ctx, "comsume stopped, try to reconnect", log.String("routeKey", routeKey), log.String("queue", queue.Name))

		err = s.retryConsume(ctx, routeKey, channel, handler)
		if err != nil {
			log.Warn(ctx, "retry consume failed", log.Err(err), log.String("routeKey", routeKey))
		}

		log.Debug(ctx, "retry consume successfully", log.String("routeKey", routeKey))
	}()

	return nil
}

func (s *RabbitMQ) retryConsume(ctx context.Context, routeKey, channel string, handler func(ctx context.Context, body []byte) error) error {
	err := s.consumeChannel.Close()
	if err != nil {
		log.Warn(ctx, "close consume channel failed", log.Err(err))
	}

	err = s.consumeConnection.Close()
	if err != nil {
		log.Warn(ctx, "close consume connection failed", log.Err(err))
	}

	s.consumeConnection, s.consumeChannel, err = s.connect(ctx)
	if err != nil {
		return err
	}

	return s.Consume(ctx, routeKey, channel, handler)
}

func (s RabbitMQ) Close() error {
	if s.publishConnection != nil {
		err := s.publishChannel.Close()
		if err != nil {
			log.Error(context.Background(), "close publish channel failed", log.Err(err))
			return err
		}

		err = s.publishConnection.Close()
		if err != nil {
			log.Error(context.Background(), "close publish connection failed", log.Err(err))
			return err
		}

		log.Info(context.Background(), "publish connection close successfully")
	}

	if s.consumeConnection != nil {
		err := s.consumeChannel.Close()
		if err != nil {
			log.Error(context.Background(), "close consume channel failed", log.Err(err))
			return err
		}

		err = s.consumeConnection.Close()
		if err != nil {
			log.Error(context.Background(), "close consume connection failed", log.Err(err))
			return err
		}

		log.Info(context.Background(), "consume connection close successfully")
	}

	log.Info(context.Background(), "rabbitmq instance close successfully")

	return nil
}
