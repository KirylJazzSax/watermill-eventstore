package esdb

import (
	"context"
	"errors"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type SubscriberConfig struct {
	ConnectionString string
	StreamConfig     SubscribeStreamConfig
	Marshaler        Marshaler
}

type SubscribeStreamConfig struct {
	SubscribeToStreamOptions esdb.SubscribeToStreamOptions
}

type Subscriber struct {
	client       *esdb.Client
	config       SubscriberConfig
	subscriberWg *sync.WaitGroup
	logger       watermill.LoggerAdapter
	stream       *esdb.Subscription
	mutex        sync.Mutex
	closing      chan struct{}
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	db, err := NewClient(config.ConnectionString, logger)
	if err != nil {
		logger.Error("conldn't connect to client", err, watermill.LogFields{
			"connectionString": config.ConnectionString,
		})
		return nil, errors.New("conldn't connect to client")
	}

	return &Subscriber{
		client:       db,
		config:       config,
		subscriberWg: &sync.WaitGroup{},
		logger:       logger,
		stream:       nil,
		mutex:        sync.Mutex{},
		closing:      make(chan struct{}),
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := s.client.SubscribeToStream(ctx, topic, s.config.StreamConfig.SubscribeToStreamOptions)
	if err != nil {
		cancel()
		s.logger.Error("can't subscribe to stream", err, watermill.LogFields{
			"topic": topic,
		})
		return nil, errors.New("can't subscribe to stream")
	}
	s.stream = stream

	out := make(chan *message.Message)
	s.subscriberWg.Add(1)

	go func() {
		defer close(out)
		defer cancel()
		defer s.subscriberWg.Done()

		for {
			select {
			case <-s.closing:
				s.logger.Debug("closing subscriber", watermill.LogFields{
					"topic": topic,
				})
				return
			case <-ctx.Done():
				s.logger.Debug("done", watermill.LogFields{
					"topic": topic,
				})
				return
			default:
				event := stream.Recv()

				if event.SubscriptionDropped != nil {
					s.logger.Debug("subscription dropped", watermill.LogFields{
						"event": event,
						"topic": topic,
					})
					break
				}

				if event.EventAppeared != nil {
					m, err := s.config.Marshaler.Unmarshal(event.EventAppeared)
					if err != nil {
						s.logger.Error("couldn't unmarshal message", err, watermill.LogFields{
							"event": event.EventAppeared,
						})

						continue
					}

					s.sendMessage(ctx, m, out)
				}
			}
		}
	}()

	return out, nil
}

func (s *Subscriber) sendMessage(
	ctx context.Context,
	m *message.Message,
	out chan *message.Message,
) {
	msgCtx, msgCancel := context.WithCancel(ctx)
	m.SetContext(msgCtx)
	defer msgCancel()

ResendLoop:
	for {
		select {
		case out <- m:
		case <-s.closing:
			s.logger.Debug("closing subscriber", watermill.LogFields{
				"message": m,
			})
			return
		case <-ctx.Done():
			s.logger.Debug("done", watermill.LogFields{
				"message": m,
			})
			return
		}

		select {
		case <-m.Acked():
			break ResendLoop
		case <-m.Nacked():
			m = m.Copy()
			m.SetContext(msgCtx)
			continue ResendLoop
		case <-s.closing:
			break ResendLoop
		case <-ctx.Done():
			break ResendLoop
		}
	}
}

func (s *Subscriber) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.stream == nil {
		return nil
	}

	s.stream.Close()
	s.closing <- struct{}{}

	return nil
}
