package esdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	client *esdb.Client
	config Config
}

func NewPublisher(config Config, logger watermill.LoggerAdapter) (*Publisher, error) {
	db, err := newClient(config.ConnectionString, logger)
	if err != nil {
		logger.Error("conldn't connect to client", err, watermill.LogFields{
			"connectionString": config.ConnectionString,
		})
		return nil, errors.New("conldn't connect to client")
	}

	return &Publisher{
		client: db,
		config: config,
	}, nil
}

func (p *Publisher) Publish(stream string, messages ...*message.Message) (err error) {
	for _, m := range messages {
		eventData, err := p.config.Marshaler.Marshal(m)
		if err != nil {
			return errors.New("couldn't marshal message")
		}

		_, err = p.client.AppendToStream(
			context.Background(),
			stream,
			p.config.Publisher.Options,
			eventData,
		)

		if err != nil {
			return fmt.Errorf("could not publish message %s", err)
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.client.Close()
}
