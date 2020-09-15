package rabbitMQ

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

const (
	reconnectDelay   = 5 * time.Second // 连接断开后多久重连
	reconnectMaxTime = 3               // 发送消息或订阅时，等待重连次数
	resendDelay      = 5 * time.Second // 消息发送失败后，多久重发
	resendTime       = 3               // 消息重发次数
)

var RabbitMQ Rabbit

//订阅回调函数
type RabbitMQTopicCallback func(string)

// 定义RabbitMQ对象
type Rabbit struct {
	connection          *amqp.Connection
	channel             *amqp.Channel
	isConnected         bool             //是否已连接
	done                chan bool        //正常关闭
	notifyClose         chan *amqp.Error //异常关闭
	url                 string           //rabbitMQ地址
	SubScribeTopicArray []SubScribeTopic //订阅事件
}

type SubScribeTopic struct {
	Name string
	Key  []string
	Func RabbitMQTopicCallback
}

func (r *Rabbit) Init(url string) error {
	r.done = make(chan bool)
	r.isConnected = false
	r.url = url

	if err := r.Conn(); err != nil {
		return errors.New("rabbitMQ connect fail")
	}

	go r.Connect()
	return nil
}

// 链接rabbitMQ
func (r *Rabbit) Connect() {
	for {
		if !r.isConnected {
			log.Println("[rabbitMQ]", "connect rabbitMQ")
			if err := r.Conn(); err != nil {
				log.Println("[rabbitMQ]", "Failed to connect rabbitMQ. Retrying...")
			}
		}

		select {
		case <-r.done:
			return
		case <-r.notifyClose:
			if r.isConnected {
				r.channel.Close()
				r.connection.Close()
				r.isConnected = false
			}
		}
		time.Sleep(reconnectDelay)
	}
}

func (r *Rabbit) Conn() error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	r.ConnSuccess(conn, channel)
	return nil
}

func (r *Rabbit) ConnSuccess(conn *amqp.Connection, channel *amqp.Channel) {

	r.connection = conn
	r.channel = channel

	r.isConnected = true

	r.notifyClose = make(chan *amqp.Error)
	r.channel.NotifyClose(r.notifyClose)

	//重新执行订阅
	r.SubscribeTopicRun()
}

//发送数据或订阅时候 等待重连
func (r *Rabbit) waitConn() error {
	i := 1
	for {
		if i >= reconnectMaxTime {
			goto END
		}
		if r.isConnected {
			goto END
		}
		i++
		time.Sleep(reconnectDelay)
	}

END:
	if r.isConnected {
		return nil
	}
	return errors.New("connect rabbitMQ fail")
}

// 关闭RabbitMQ连接
func (r *Rabbit) Close() {
	close(r.done)

	if r.isConnected {
		r.channel.Close()
		r.connection.Close()
		r.isConnected = false
	}
}

// topic订阅回调函数
func (r *Rabbit) SubscribeTopic(cb RabbitMQTopicCallback, name string, key ...string) {
	r.SubScribeTopicArray = append(r.SubScribeTopicArray, SubScribeTopic{
		Name: name,
		Key:  key,
		Func: cb,
	})
}

// 执行订阅
func (r *Rabbit) SubscribeTopicRun() {
	for _, v := range r.SubScribeTopicArray {
		if err := r.SubscribeTopicAction(v.Func, v.Name, v.Key...); err != nil {
			log.Println("[subscribe]", "订阅失败", v.Name, v.Key)
		}
	}
	return
}

// topic订阅回调函数
func (r *Rabbit) SubscribeTopicAction(cb RabbitMQTopicCallback, name string, key ...string) error {
	var err error
	if err = r.waitConn(); err != nil {
		return err
	}

	kind := "topic"
	err = r.channel.ExchangeDeclare(
		name,  // name
		kind,  // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	q, err := r.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	for _, s := range key {
		err = r.channel.QueueBind(
			q.Name, // queue name
			s,      // routing key
			name,   // exchange
			false,
			nil)

		if err != nil {
			return err
		}
	}

	err = r.channel.QueueBind(
		q.Name, // queue name
		"",     // routing key
		name,   // exchange
		false,
		nil)
	if err != nil {
		return err
	}

	msgs, err := r.channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			cb(fmt.Sprintf("%s", d.Body))
		}
	}()

	log.Println("[subscribe]", kind, name, key)
	return nil
}

// 发送订阅消息
func (r Rabbit) PublicTopic(name, key, body string) {
	go func() {
		var err error
		i := 1
		for {
			if i >= resendTime {
				goto END
			}
			err = r.PublicTopicAction(name, key, body)
			if err == nil {
				log.Println("public", name, key)
				goto END
			}
			time.Sleep(resendDelay)
			i++
		}
	END:
		if err != nil {
			log.Println("public", "err", name, key, err)
		}
	}()
}

func (r Rabbit) PublicTopicAction(name, key, body string) error {
	var err error
	if err = r.waitConn(); err != nil {
		return err
	}

	kind := "topic"
	err = r.channel.ExchangeDeclare(
		name,  // name
		kind,  // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}
	err = r.channel.Publish(
		name,  // exchange
		key,   // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	return err
}
