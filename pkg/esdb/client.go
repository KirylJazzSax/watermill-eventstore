package esdb

import (
	"errors"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/ThreeDotsLabs/watermill"
)

func newClient(connectionString string, logger watermill.LoggerAdapter) (*esdb.Client, error) {
	settings, err := esdb.ParseConnectionString(connectionString)

	if err != nil {
		logger.Error("conldn't parse connection string", err, watermill.LogFields{
			"connectionString": connectionString,
		})
		return nil, errors.New("conldn't parse connection string")
	}

	db, err := esdb.NewClient(settings)
	if err != nil {
		logger.Error("conldn't connect to client", err, watermill.LogFields{
			"connectionString": connectionString,
		})
		return nil, errors.New("conldn't connect to client")
	}

	return db, nil
}
