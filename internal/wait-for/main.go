package main

import (
	"context"
	"fmt"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
)

func main() {
	settings, err := esdb.ParseConnectionString("esdb+discover://localhost?tls=true&tlsVerifyCert=false")
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
				Login:    "ops",
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
