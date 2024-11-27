//go:build stress
// +build stress

package esdb_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPubSubStress(t *testing.T) {
	tests.TestPubSubStressTest(
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

func TestPubSubStressPersistentAndConsumerGroups(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSub,
		createPubSubPersistentWithConsumerGroups,
	)
}
