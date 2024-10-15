package esdb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

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
	closing      chan struct{}
	closeFunc    func() error
}

func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	client, err := NewClient(config.ConnectionString, logger)
	if err != nil {
		logger.Error("conldn't connect to client", err, watermill.LogFields{
			"connectionString": config.ConnectionString,
		})
		return nil, errors.New("conldn't connect to client")
	}

	closing := make(chan struct{})
	subscriberWg := &sync.WaitGroup{}
	var closed uint32
	closeFunc := func() error {
		if !atomic.CompareAndSwapUint32(&closed, 0, 1) {
			return nil
		}

		close(closing)
		subscriberWg.Wait()

		return client.Close()
	}

	return &Subscriber{
		client,
		config,
		subscriberWg,
		logger,
		closing,
		closeFunc,
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

	out := make(chan *message.Message)
	in := make(chan *esdb.ResolvedEvent)
	go func() {
		defer func() {
			stream.Close()
			close(out)
			cancel()
			s.subscriberWg.Done()
		}()
		for {
			select {
			case <-s.closing:
				return
			case <-ctx.Done():
				return
			case event := <-in:
				m, err := s.config.Marshaler.Unmarshal(event)
				if err != nil {
					s.logger.Error("couldn't unmarshal message", err, watermill.LogFields{
						"event": event,
					})

					continue
				}

				s.sendMessage(ctx, m, out)
			}
		}
	}()
	s.subscriberWg.Add(1)

	go func() {
		for {
			event := stream.Recv()

			if event.SubscriptionDropped != nil {
				s.logger.Debug("subscription dropped", watermill.LogFields{
					"event": event,
					"topic": topic,
				})
				return
			}

			if event.EventAppeared != nil {
				in <- event.EventAppeared
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
	return s.closeFunc()
}
