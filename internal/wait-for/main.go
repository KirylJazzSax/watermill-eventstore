package main

import (
	"context"
	"fmt"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
)

func main() {
	settings, err := esdb.ParseConnectionString("esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false")
	if err != nil {
		panic(err)
	}

	client, err := esdb.NewClient(settings)
	if err != nil {
		panic(err)
	}

	for {
		_, err := client.ListAllPersistentSubscriptions(context.TODO(), esdb.ListPersistentSubscriptionsOptions{
			Authenticated: &esdb.Credentials{
				Login:    "admin",
				Password: "changeit",
			},
		})

		fmt.Println(err)

		if err == nil {
			break
		}

		time.Sleep(time.Second * 1)
	}

	fmt.Println("done")
}
