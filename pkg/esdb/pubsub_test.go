package esdb_test

import (
	"testing"

	wesdb "github.com/KirylJazzSax/watermill-eventstore/pkg/esdb"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/google/uuid"
)

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    "ops",
		Password: "changeit",
	}
	connectionString := "esdb+discover://localhost:2113?tls=true&tlsVerifyCert=false"
	marshaler := wesdb.DefaultMarshaler{}

	pub, err := wesdb.NewPublisher(wesdb.PublisherConfig{
		ConnectionString: connectionString,
		Marshaler:        marshaler,
		StreamConfig: wesdb.PublishStreamConfig{
			Options: esdb.AppendToStreamOptions{
				Authenticated: credentials,
			},
		},
	}, watermill.NopLogger{})

	if err != nil {
		panic(err)
	}

	sub, err := wesdb.NewSubscriber(wesdb.SubscriberConfig{
		ConnectionString: connectionString,
		Marshaler:        marshaler,
		StreamConfig: wesdb.SubscribeStreamConfig{
			SubscribeToStreamOptions: esdb.SubscribeToStreamOptions{
				From:          esdb.Start{},
				Authenticated: credentials,
			},
		},
	}, watermill.NopLogger{})

	if err != nil {
		panic(err)
	}

	return pub, sub
}

func createPubSubPersistent(t *testing.T) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    "ops",
		Password: "changeit",
	}
	connectionString := "esdb+discover://localhost:2113?tls=true&tlsVerifyCert=false"
	marshaler := wesdb.DefaultMarshaler{}

	pub, err := wesdb.NewPublisher(wesdb.PublisherConfig{
		ConnectionString: connectionString,
		Marshaler:        marshaler,
		StreamConfig: wesdb.PublishStreamConfig{
			Options: esdb.AppendToStreamOptions{
				Authenticated: credentials,
			},
		},
	}, watermill.NopLogger{})

	if err != nil {
		panic(err)
	}

	u, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	subGroup := u.String()
	sub, err := wesdb.NewSubscriber(wesdb.SubscriberConfig{
		ConnectionString: connectionString,
		Marshaler:        marshaler,
		StreamConfig: wesdb.SubscribeStreamConfig{
			SubscriptionGroup: subGroup,
			SubscribeToPersistentSubscriptionOptions: esdb.SubscribeToPersistentSubscriptionOptions{
				Authenticated: credentials,
			},
			PersistentStreamSubscriptionOptions: esdb.PersistentStreamSubscriptionOptions{
				StartFrom:     esdb.Start{},
				Authenticated: credentials,
			},
		},
	}, watermill.NopLogger{})

	if err != nil {
		panic(err)
	}

	return pub, sub
}

func createPubSubPersistentWithConsumerGroups(t *testing.T, consumerGroups string) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    "ops",
		Password: "changeit",
	}
	connectionString := "esdb+discover://localhost:2113?tls=true&tlsVerifyCert=false"
	marshaler := wesdb.DefaultMarshaler{}

	pub, err := wesdb.NewPublisher(wesdb.PublisherConfig{
		ConnectionString: connectionString,
		Marshaler:        marshaler,
		StreamConfig: wesdb.PublishStreamConfig{
			Options: esdb.AppendToStreamOptions{
				Authenticated:    credentials,
				ExpectedRevision: esdb.Any{},
			},
		},
	}, watermill.NopLogger{})

	if err != nil {
		panic(err)
	}

	sub, err := wesdb.NewSubscriber(wesdb.SubscriberConfig{
		ConnectionString: connectionString,
		Marshaler:        marshaler,
		StreamConfig: wesdb.SubscribeStreamConfig{
			SubscriptionGroup: consumerGroups,
			SubscribeToPersistentSubscriptionOptions: esdb.SubscribeToPersistentSubscriptionOptions{
				Authenticated: credentials,
			},
			PersistentStreamSubscriptionOptions: esdb.PersistentStreamSubscriptionOptions{
				StartFrom:     esdb.Start{},
				Authenticated: credentials,
			},
		},
	}, watermill.NopLogger{})

	if err != nil {
		panic(err)
	}

	return pub, sub
}

func TestPubSub(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      false,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          false,
		},
		createPubSub,
		func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) { return nil, nil },
	)
}

func TestPubSubPersistentSubscriptionAndConsumerGroups(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSubPersistent,
		createPubSubPersistentWithConsumerGroups,
	)
}
