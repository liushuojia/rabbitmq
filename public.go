package main

import (
	"fmt"
	"log"
	"rabbitMQ/rabbitMQ"
	"time"
)

func main() {

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", "admin", "admin", "127.0.0.1", 5672)

	err := rabbitMQ.RabbitMQ.Init(url)
	if err != nil {
		log.Fatalln(err)
	}

	rabbitMQ.RabbitMQ.PublicTopic("finebaas", "title", "hello world ")
	rabbitMQ.RabbitMQ.PublicTopic("finebaas", "title2", "hello world 2")

	time.Sleep(5 * time.Second)
}
