package amqp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shimmur/proto_schemas_go/rpc"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	amqpDriver "github.com/streadway/amqp"
	"go.k6.io/k6/js/modules"
)

const version = "v0.0.1"

type AMQP struct {
	Version    string
	Connection *amqpDriver.Connection
	Queue      *Queue
	Exchange   *Exchange
}

type AMQPOptions struct {
	ConnectionURL string
}

type PublishOptions struct {
	QueueName     string
	Body          []byte
	Exchange      string
	Mandatory     bool
	Immediate     bool
	ReplyTo       string
	CorrelationID string
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

func (amqp *AMQP) Encode(ctx context.Context, procedure string, destination string, source string, params map[string]interface{}) []byte {
	start := time.Now()
	args := MapToStruct(params)

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
	ObserveTrend(ctx, RPCEncoding, time.Since(start).Seconds())
	return msgbytes
}

func (amqp *AMQP) Decode(ctx context.Context, encodedMsg []byte) map[string]interface{} {
	start := time.Now()
	msg := &rpc.RPCResponse{}

	// unmarshal to event
	if err := proto.Unmarshal(encodedMsg, msg); err != nil {
		fmt.Printf("failed to decode message: %v. error: %v", encodedMsg, err)
		return nil
	}

	m := StructToMap(*msg.GetResponse())
	ObserveTrend(ctx, RPCDecoding, time.Since(start).Seconds())
	return m
}

func (amqp *AMQP) Start(options AMQPOptions) error {
	conn, err := amqpDriver.Dial(options.ConnectionURL)
	amqp.Connection = conn
	amqp.Queue.Connection = conn
	amqp.Exchange.Connection = conn
	return err
}

func (amqp *AMQP) Publish(options PublishOptions) error {
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
			CorrelationId: options.CorrelationID,
			ReplyTo:       options.ReplyTo,
			Headers:       options.Headers,
		},
	)
}

func (amqp *AMQP) Listen(options ListenOptions) error {
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

func (amqp *AMQP) ConsumeRPC(ctx context.Context, opts ConsumeOptions) []map[string]interface{} {
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

	c := make(chan amqpDriver.Delivery, 1)
	go func() {
		for m := range msgs {
			c <- m
		}
	}()

	for {
		select {
		case res := <-c:
			tags := generateRPCTags(res)

			decoded := amqp.Decode(ctx, res.Body)
			if decoded != nil {
				messages = append(messages, decoded)
			}

			IncrementCounterWithTags(ctx, RPCConsumeCounter, tags)

			if len(messages) >= opts.MessageCount {
				close(c)
				return messages
			}
		case <-time.After(time.Duration(opts.Timeout) * time.Millisecond):
			fmt.Println("timeout")
			return nil
		}
	}
}

func (amqp *AMQP) ConsumeSingleRPC(ctx context.Context, opts ConsumeOptions) interface{} {
	opts.MessageCount = 1
	messages := amqp.ConsumeRPC(ctx, opts)
	if messages != nil {
		return messages[0]
	}

	return nil
}

func (amqp *AMQP) PublishRPC(ctx context.Context, pubOpts PublishOptions, procedure string, destination string, source string, params map[string]interface{}) error {
	encoded := amqp.Encode(ctx, procedure, destination, source, params)

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

	tags := map[string]string{
		"source":     source,
		"desination": destination,
		"procedure":  procedure,
		"exchange":   pubOpts.Exchange,
	}
	IncrementCounterWithTags(ctx, RPCPublishCounter, tags)

	amqp.Publish(pubOpts)
	return nil
}

func generateRPCTags(msg amqpDriver.Delivery) map[string]string {
	t := map[string]string{}

	t["exchange"] = msg.Exchange
	t["routing_key"] = msg.RoutingKey
	t["reply_to"] = msg.ReplyTo

	if val, ok := msg.Headers["destination"]; ok {
		t["destination"] = val.(string)
	}

	return t
}

func init() {
	queue := Queue{}
	exchange := Exchange{}
	generalAMQP := AMQP{
		Version:  version,
		Queue:    &queue,
		Exchange: &exchange,
	}

	modules.Register("k6/x/amqp", &generalAMQP)
	modules.Register("k6/x/amqp/queue", &queue)
	modules.Register("k6/x/amqp/exchange", &exchange)
}
