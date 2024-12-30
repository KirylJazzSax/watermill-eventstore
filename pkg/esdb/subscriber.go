package esdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
	client       *esdb.Client
	config       Config
	subscriberWg *sync.WaitGroup
	logger       watermill.LoggerAdapter
	closing      chan struct{}
	closeFunc    func() error
}

func NewSubscriber(config Config, logger watermill.LoggerAdapter) (*Subscriber, error) {
	client, err := newClient(config.ConnectionString, logger)
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

func (s *Subscriber) createPersistentSubscription(ctx context.Context, topic string) error {
	err := s.client.CreatePersistentSubscription(
		ctx,
		topic,
		s.config.Subscriber.SubscriptionGroup,
		s.config.Subscriber.PersistentStreamSubscriptionOptions,
	)

	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") {
			s.logger.Info("supscription already exists", watermill.LogFields{
				"topic":              topic,
				"subscription-group": s.config.Subscriber.SubscriptionGroup,
			})
		} else {
			s.logger.Error("can't create persistent subscription", err, watermill.LogFields{
				"topic":              topic,
				"subscription-group": s.config.Subscriber.SubscriptionGroup,
			})
			return errors.New("can't create persistent subscription")
		}
	}

	return nil
}

func (s *Subscriber) handlePersistentSubscription(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ctx, cancel := context.WithCancel(ctx)
	err := s.createPersistentSubscription(ctx, topic)

	if err != nil {
		cancel()
		return nil, err
	}

	stream, err := s.client.SubscribeToPersistentSubscription(
		ctx,
		topic,
		s.config.Subscriber.SubscriptionGroup,
		s.config.Subscriber.SubscribeToPersistentSubscriptionOptions,
	)

	if err != nil {
		cancel()
		s.logger.Error("can't subscribe to stream", err, watermill.LogFields{
			"topic": topic,
		})
		return nil, errors.New("can't subscribe to stream")
	}

	out := make(chan *message.Message)
	in := make(chan *esdb.ResolvedEvent)
	s.subscriberWg.Add(1)

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
				if success := s.sendMessage(ctx, m, out); success {
					err := stream.Ack(event)
					if err != nil {
						s.logger.Error("couldn't acc message", err, watermill.LogFields{
							"event": event,
						})
					}
				} else {
					err := stream.Nack(fmt.Sprintf("nack event %s", m.UUID), esdb.NackActionSkip, event)
					if err != nil {
						s.logger.Error("couldn't nack message", err, watermill.LogFields{
							"event": event,
						})
					}
				}
			}
		}
	}()

	go func() {
		defer close(in)
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
				in <- event.EventAppeared.Event
			}
		}
	}()

	return out, nil
}

func (s *Subscriber) handleCatchUpSubscription(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := s.client.SubscribeToStream(ctx, topic, s.config.Subscriber.SubscribeToStreamOptions)
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
		defer close(in)
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

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.config.Subscriber.SubscriptionGroup != "" {
		return s.handlePersistentSubscription(ctx, topic)
	}

	return s.handleCatchUpSubscription(ctx, topic)
}

func (s *Subscriber) sendMessage(
	ctx context.Context,
	m *message.Message,
	out chan *message.Message,
) bool {
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
			return false
		case <-ctx.Done():
			s.logger.Debug("done", watermill.LogFields{
				"message": m,
			})
			return false
		}

		select {
		case <-m.Acked():
			return true
		case <-m.Nacked():
			m = m.Copy()
			m.SetContext(msgCtx)
			continue ResendLoop
		case <-s.closing:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

func (s *Subscriber) Close() error {
	return s.closeFunc()
}
