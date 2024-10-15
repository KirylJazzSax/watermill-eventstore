package esdb

import (
	"encoding/json"
	"errors"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	DefaultMessageUUIDHeaderKey = "_watermill_message_uuid"
	DefaultEventTypeKey         = "_watermill_event_type"
	DefaultEventType            = "watermill_event"
)

type Marshaler interface {
	Marshal(msg *message.Message) (esdb.EventData, error)
	Unmarshal(event *esdb.ResolvedEvent) (*message.Message, error)
}

type DefaultMarshaler struct {
	MessageUUIDHeaderKey string
	EventTypeKey         string
	EventType            string
}

func (d DefaultMarshaler) Marshal(msg *message.Message) (esdb.EventData, error) {
	eventType := msg.Metadata.Get(DefaultEventTypeKey)
	if eventType == "" {
		eventType = DefaultEventType
	}

	eventMetadata := msg.Copy().Metadata
	eventMetadata.Set(DefaultMessageUUIDHeaderKey, msg.UUID)

	marshaledMetadata, err := json.Marshal(eventMetadata)

	if err != nil {
		return esdb.EventData{}, errors.New("can't encode message metadata")
	}

	return esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   eventType,
		Data:        msg.Payload,
		Metadata:    marshaledMetadata,
	}, nil
}

func (d DefaultMarshaler) Unmarshal(event *esdb.ResolvedEvent) (*message.Message, error) {
	var metadata message.Metadata

	err := json.Unmarshal(event.Event.UserMetadata, &metadata)
	if err != nil {
		return nil, errors.New("couldn't decode metadata")
	}

	m := message.NewMessage(metadata.Get(DefaultMessageUUIDHeaderKey), event.Event.Data)
	m.Metadata = metadata
	return m, nil
}
