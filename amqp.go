package amqp

import (
	"errors"
	"fmt"
	"time"

	"github.com/Shimmur/proto_schemas_go/rpc"
	// "google.golang.org/protobuf/proto"

	// "github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	amqpDriver "github.com/streadway/amqp"
	"go.k6.io/k6/js/modules"
)

const version = "v0.0.1"

type Amqp struct {
	Version    string
	Connection *amqpDriver.Connection
	Queue      *Queue
	Exchange   *Exchange
}

type AmqpOptions struct {
	ConnectionUrl string
}

type PublishOptions struct {
	QueueName     string
	Body          []byte
	Exchange      string
	Mandatory     bool
	Immediate     bool
	ReplyTo       string
	CorrelationId string
	Headers       amqpDriver.Table
}

type ConsumeOptions struct {
	Consumer     string
	QueueName    string
	AutoAck      bool
	Exclusive    bool
	NoLocal      bool
	NoWait       bool
	Args         amqpDriver.Table
	MessageCount int
	Timeout      int
}

type ListenerType func([]byte) error

type ListenOptions struct {
	Listener  ListenerType
	QueueName string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqpDriver.Table
}

func (amqp *Amqp) Encode(procedure string, destination string, source string, params map[string]interface{}) []byte {
	args := Map_to_struct(params)

	rpc := &rpc.RPC{
		Type:        rpc.RPC_WITH_REPLY,
		Procedure:   procedure,
		Destination: destination,
		Source:      source,
		Id:          uuid.NewString(),
		Timestamp:   time.Now().UnixMicro(),
		Args:        args,
	}

	msgbytes, err := proto.Marshal(rpc)
	if err != nil {
		return nil
	}

	return msgbytes
}

func (amqp *Amqp) Decode(encodedMsg []byte) map[string]interface{} {
	msg := &rpc.RPCResponse{}

	// unmarshal to event
	if err := proto.Unmarshal(encodedMsg, msg); err != nil {
		fmt.Printf("failed to decode message: %v. error: %v", encodedMsg, err)
		return nil
	}

	m := Struct_to_map(*msg.GetResponse())
	// fmt.Printf("hi: %v", m)
	return m
}

func (amqp *Amqp) Start(options AmqpOptions) error {
	conn, err := amqpDriver.Dial(options.ConnectionUrl)
	amqp.Connection = conn
	amqp.Queue.Connection = conn
	amqp.Exchange.Connection = conn
	return err
}

func (amqp *Amqp) Publish(options PublishOptions) error {
	ch, err := amqp.Connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.Publish(
		options.Exchange,
		options.QueueName,
		options.Mandatory,
		options.Immediate,
		amqpDriver.Publishing{
			ContentType:   "text/plain",
			Body:          options.Body,
			CorrelationId: options.CorrelationId,
			ReplyTo:       options.ReplyTo,
			Headers:       options.Headers,
		},
	)
}

func (amqp *Amqp) Listen(options ListenOptions) error {
	ch, err := amqp.Connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		options.QueueName,
		options.Consumer,
		options.AutoAck,
		options.Exclusive,
		options.NoLocal,
		options.NoWait,
		options.Args,
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			options.Listener(d.Body)
		}
	}()
	return nil
}

func (amqp *Amqp) ConsumeRPC(opts ConsumeOptions) []map[string]interface{} {
	ch, err := amqp.Connection.Channel()
	if err != nil {
		fmt.Printf("errored opening channel: %v\n", err)
		return nil
	}
	defer ch.Close()

	if opts.MessageCount == 0 {
		opts.MessageCount = 1
	}

	if opts.Timeout == 0 {
		opts.Timeout = 1000
	}

	if opts.Consumer == "" {
		opts.Consumer = "k6-consumer-auto"
	}

	msgs, err := ch.Consume(
		opts.QueueName,
		opts.Consumer,
		true, //auto ack
		opts.Exclusive,
		opts.NoLocal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		fmt.Printf("errored consuming: %v\n", err)
		return nil
	}

	var messages []map[string]interface{}

	c := make(chan map[string]interface{}, 2)
	go func() {
		for m := range msgs {
			decoded := amqp.Decode(m.Body)
			if decoded != nil {
				c <- decoded
			}
		}
	}()

	for {
		select {
		case res := <-c:
			// fmt.Printf("got body: %v\n\n", res)
			messages = append(messages, res)

			if len(messages) >= opts.MessageCount {
				close(c)
				// fmt.Print("max message count")
				return messages
			}
		case <-time.After(time.Duration(opts.Timeout) * time.Millisecond):
			fmt.Println("timeout")
			return nil
		}
	}
}

func (amqp *Amqp) ConsumeSingleRPC(opts ConsumeOptions) interface{} {
	opts.MessageCount = 1
	messages := amqp.ConsumeRPC(opts)
	if messages != nil {
		return messages[0]
	}

	return nil
}

func (amqp *Amqp) PublishRPC(pubOpts PublishOptions, procedure string, destination string, source string, params map[string]interface{}) error {
	encoded := amqp.Encode(procedure, destination, source, params)

	if encoded == nil {
		return errors.New("failed to encode rpc message")
	}
	pubOpts.Body = encoded

	if pubOpts.Exchange == "" {
		pubOpts.Exchange = "rpc"
	}

	if pubOpts.Headers == nil {
		pubOpts.Headers = amqpDriver.Table{}
	}
	pubOpts.Headers["destination"] = destination

	amqp.Publish(pubOpts)
	return nil
}

func init() {

	queue := Queue{}
	exchange := Exchange{}
	generalAmqp := Amqp{
		Version:  version,
		Queue:    &queue,
		Exchange: &exchange,
	}

	modules.Register("k6/x/amqp", &generalAmqp)
	modules.Register("k6/x/amqp/queue", &queue)
	modules.Register("k6/x/amqp/exchange", &exchange)
}
