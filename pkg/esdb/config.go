package esdb

import (
	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/google/uuid"
)

type PublisherConfig struct {
	Options esdb.AppendToStreamOptions
}

type SubscriberConfig struct {
	SubscribeToStreamOptions                 esdb.SubscribeToStreamOptions
	SubscribeToPersistentSubscriptionOptions esdb.SubscribeToPersistentSubscriptionOptions
	PersistentStreamSubscriptionOptions      esdb.PersistentStreamSubscriptionOptions
	SubscriptionGroup                        string
}

type Config struct {
	ConnectionString string
	Publisher        PublisherConfig
	Subscriber       SubscriberConfig
	Marshaler        Marshaler
}

// Config for simple catch up subcription.
func NewCatchUpConfig(
	connectionString string,
	credentials *esdb.Credentials,
	from esdb.StreamPosition,
) Config {
	return Config{
		ConnectionString: connectionString,
		Marshaler:        DefaultMarshaler{},
		Publisher: PublisherConfig{
			Options: esdb.AppendToStreamOptions{
				Authenticated: credentials,
			},
		},
		Subscriber: SubscriberConfig{
			SubscribeToStreamOptions: esdb.SubscribeToStreamOptions{
				From:          from,
				Authenticated: credentials,
			},
		},
	}
}

// Config for persistent subscription.
// To create a persistent subscription, we need a subscription group (consumer group).
// Here, we generate it.
func NewPersistentSubscriptionConfig(
	connectionString string,
	credentials *esdb.Credentials,
	from esdb.StreamPosition,
) (Config, error) {
	u, err := uuid.NewUUID()
	if err != nil {
		return Config{}, err
	}
	subscriptionGroup := u.String()

	return Config{
		ConnectionString: connectionString,
		Marshaler:        DefaultMarshaler{},
		Publisher: PublisherConfig{
			Options: esdb.AppendToStreamOptions{
				Authenticated: credentials,
			},
		},
		Subscriber: SubscriberConfig{
			SubscriptionGroup: subscriptionGroup,
			SubscribeToPersistentSubscriptionOptions: esdb.SubscribeToPersistentSubscriptionOptions{
				Authenticated: credentials,
			},
			PersistentStreamSubscriptionOptions: esdb.PersistentStreamSubscriptionOptions{
				StartFrom:     from,
				Authenticated: credentials,
			},
		},
	}, nil
}

// Configuration for a persistent subscription with a consumer group.
func NewPersistentSubscriptionConsumerGroupConfig(
	connectionString string,
	consumerGroup string,
	credentials *esdb.Credentials,
	from esdb.StreamPosition,
) Config {
	return Config{
		ConnectionString: connectionString,
		Marshaler:        DefaultMarshaler{},
		Publisher: PublisherConfig{
			Options: esdb.AppendToStreamOptions{
				Authenticated: credentials,
			},
		},
		Subscriber: SubscriberConfig{
			SubscriptionGroup: consumerGroup,
			SubscribeToPersistentSubscriptionOptions: esdb.SubscribeToPersistentSubscriptionOptions{
				Authenticated: credentials,
			},
			PersistentStreamSubscriptionOptions: esdb.PersistentStreamSubscriptionOptions{
				StartFrom:     from,
				Authenticated: credentials,
			},
		},
	}
}
