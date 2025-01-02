package esdb_test

import (
	"testing"

	wesdb "github.com/KirylJazzSax/watermill-eventstore/pkg/esdb"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

// default login and password that set in eventstoredb
const (
	login    = "ops"
	password = "changeit"
)

const connectionString = "esdb+discover://localhost:2113?tls=true&tlsVerifyCert=false"

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    login,
		Password: password,
	}
	config := wesdb.NewCatchUpConfig(connectionString, credentials, esdb.Start{})

	pub, err := wesdb.NewPublisher(config, watermill.NewStdLogger(true, true))

	if err != nil {
		panic(err)
	}

	sub, err := wesdb.NewSubscriber(config, watermill.NewStdLogger(true, true))

	if err != nil {
		panic(err)
	}

	return pub, sub
}

func createPubSubPersistent(t *testing.T) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    login,
		Password: password,
	}

	config, err := wesdb.NewPersistentSubscriptionConfig(connectionString, credentials, esdb.Start{})

	if err != nil {
		panic(err)
	}

	pub, err := wesdb.NewPublisher(config, watermill.NewStdLogger(true, true))

	if err != nil {
		panic(err)
	}

	sub, err := wesdb.NewSubscriber(config, watermill.NewStdLogger(true, true))

	if err != nil {
		panic(err)
	}

	return pub, sub
}

func createPubSubPersistentWithConsumerGroups(t *testing.T, consumerGroups string) (message.Publisher, message.Subscriber) {
	credentials := &esdb.Credentials{
		Login:    login,
		Password: password,
	}
	config := wesdb.NewPersistentSubscriptionConsumerGroupConfig(
		connectionString,
		consumerGroups,
		credentials,
		esdb.Start{},
	)

	pub, err := wesdb.NewPublisher(config, watermill.NewStdLogger(true, true))

	if err != nil {
		panic(err)
	}

	sub, err := wesdb.NewSubscriber(config, watermill.NewStdLogger(true, true))

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
