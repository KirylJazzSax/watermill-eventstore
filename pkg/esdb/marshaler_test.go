package esdb_test

import (
	"encoding/json"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	wesdb "github.com/KirylJazzSax/watermill-eventstore/pkg/esdb"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	marshaler := wesdb.DefaultMarshaler{}
	messageToMarshal := message.NewMessage(watermill.NewUUID(), []byte("hello"))

	eventData, err := marshaler.Marshal(messageToMarshal)
	require.NoError(t, err)
	assert.Equal(t, esdb.ContentTypeJson, eventData.ContentType)
}

func TestDefaultEventType(t *testing.T) {
	marshaler := wesdb.DefaultMarshaler{}
	messageToMarshal := message.NewMessage(watermill.NewUUID(), []byte("hello"))

	eventData, _ := marshaler.Marshal(messageToMarshal)
	metadataBytes := eventData.Metadata

	var metadata message.Metadata
	err := json.Unmarshal(metadataBytes, &metadata)
	require.NoError(t, err)

	assert.Equal(t, wesdb.DefaultEventType, eventData.EventType)
}

func TestEventType(t *testing.T) {
	marshaler := wesdb.DefaultMarshaler{}
	messageToMarshal := message.NewMessage(watermill.NewUUID(), []byte("hello"))
	messageToMarshal.Metadata.Set(wesdb.DefaultEventTypeKey, "neweventtype")

	eventData, _ := marshaler.Marshal(messageToMarshal)
	metadataBytes := eventData.Metadata

	var metadata message.Metadata
	err := json.Unmarshal(metadataBytes, &metadata)
	require.NoError(t, err)

	assert.Equal(t, "neweventtype", eventData.EventType)
}

func TestUnmarshal(t *testing.T) {
	marshaler := wesdb.DefaultMarshaler{}
	data := struct {
		Field string
	}{
		Field: "hello",
	}
	marshaledData, err := json.Marshal(data)
	require.NoError(t, err)
	messageToMarshal := message.NewMessage(watermill.NewUUID(), marshaledData)

	eventData, err := marshaler.Marshal(messageToMarshal)
	require.NoError(t, err)

	resolvedEvent := &esdb.ResolvedEvent{
		Event: &esdb.RecordedEvent{
			UserMetadata: eventData.Metadata,
			Data:         marshaledData,
		},
	}

	unmarshaledMessage, err := marshaler.Unmarshal(resolvedEvent)
	require.NoError(t, err)
	var unmarshaledBody struct {
		Field string
	}

	json.Unmarshal(unmarshaledMessage.Payload, &unmarshaledBody)
	assert.Equal(t, data, unmarshaledBody)
}
