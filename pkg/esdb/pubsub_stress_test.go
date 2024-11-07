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
