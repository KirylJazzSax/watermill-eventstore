package esdb_test

import (
	"testing"
	wesdb "watermill-eventstore/pkg/esdb"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    "ops",
		Password: "changeit",
	}
	connectionString := "esdb://localhost:2113?tls=true&tlsVerifyCert=false"
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

func TestPubSub(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			// TODO: implement consumer groups
			ConsumerGroups: false,
			// maybe its possible with persistent subscriptions
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			// TODO: implement with persistent
			Persistent: false,
		},
		createPubSub,
		func(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) { return nil, nil },
	)
}
