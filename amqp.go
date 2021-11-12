package amqp

import (
	"fmt"
	"time"

	"github.com/Shimmur/proto_schemas_go/rpc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	amqpDriver "github.com/streadway/amqp"
	"go.k6.io/k6/js/modules"
)

const version = "v0.0.1"

var (
	pbJSONMarshaler = &jsonpb.Marshaler{OrigName: true}
)

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
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqpDriver.Table
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
	// m is a map[string]interface.
	// loop over keys and values in the map.
	for k, v := range params {
		fmt.Println(k, "value is", v)
	}
	args := Map_to_struct(params)
	fmt.Printf("build: %v \n", args)
	event := &rpc.RPC{
		Type:        rpc.RPC_WITH_REPLY,
		Procedure:   procedure,
		Destination: destination,
		Source:      source,
		Id:          uuid.NewString(),
		Timestamp:   time.Now().UnixMicro(),
		Args:        args,
		// Args: &types.Struct{
		// 	Fields: map[string]*types.Value{
		// 		"thread_id": {
		// 			Kind: &types.Value_StringValue{StringValue: "6025321b-7f3a-4f9c-99f9-9581c6e6f734"},
		// 		},
		// 		"sms_campaign_id": {
		// 			Kind: &types.Value_StringValue{StringValue: "51628409-300f-4beb-b18c-1f1baabbfa8a"},
		// 		},
		// 	},
		// },
	}

	fmt.Printf("%+v\n", event)

	msgbytes, err := proto.Marshal(event)
	if err != nil {
		return nil
	}

	return msgbytes
}

func (amqp *Amqp) Decode(encodedMsg []byte) map[string]interface{} {
	// var msg *rpc.RPCResponse // this is what you want
	msg := &rpc.RPCResponse{} // all events on the bus are wrapped in this

	// unmarshal to event
	if err := proto.Unmarshal(encodedMsg, msg); err != nil {
		fmt.Printf("failed to decode message: %v. error: %v", encodedMsg, err)
		return nil
	}

	// var buf bytes.Buffer

	// err := pbJSONMarshaler.Marshal(&buf, msg)

	// if err != nil {
	// 	fmt.Printf("failed to decode json: %v. error: %v", encodedMsg, err)
	// 	return nil
	// }
	// m := make(map[string]interface{})

	// m["something"] = 1
	// m["another"] = "hello there"
	m := Struct_to_map(*msg.GetResponse())
	fmt.Printf("hi: %v", m)
	return m
	// return msg.GetResponse()
	// return buf.Bytes()
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
