package main

import (
	"fmt"
	"log"
	"rabbitMQ/rabbitMQ"
)

func main() {

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "admin", "admin", "127.0.0.1", 5672)

	err := rabbitMQ.RabbitMQ.Init(url)
	if err != nil {
		log.Fatalln(err)
	}

	rabbitMQ.RabbitMQ.SubscribeTopic(subscribe, "finebaas", "title")
	rabbitMQ.RabbitMQ.SubscribeTopic(subscribeOther, "finebaas", "title2")

	rabbitMQ.RabbitMQ.SubscribeTopicRun()

	select {}
}

func subscribe(msg string) {
	log.Println("subscribe", msg)
}

func subscribeOther(msg string) {
	log.Println("subscribe", "other", msg)
}
