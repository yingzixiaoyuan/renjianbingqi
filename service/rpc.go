package service

import (
	"os"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"go.uber.org/zap"
)

type Connection struct {
	Url         string
	Mc          *amqp.Connection
	Chs         map[string]*Channel
	Quit        chan bool
	NotifyClose chan *amqp.Error
}

type Channel struct {
	Name              string
	Qos               int
	Channel           *amqp.Channel
	Connection        *Connection
	MsgSequenceNumber uint64
	ConfirmedUpTo     uint64
	UnconfirmedMsg    map[uint64]func(*Err)
}

func (m *Channel) Publish(exchange string, route string, mandatory, immediate bool, props amqp.Publishing, callback func(*Err)) {
	if m.ChannelIsEmpty() {
		zap.L().Sugar().Errorf("channel on publish is empty")
		callback(RabbitmqError())
		return
	}
	if m.Connection.ConnlIsEmpty() {
		zap.L().Sugar().Errorf("connection is not ready")
		callback(RabbitmqError())
		return
	}
	atomic.AddUint64(&m.MsgSequenceNumber, 1)
	props.DeliveryMode = amqp.Persistent
	props.MessageId = strconv.FormatUint(m.MsgSequenceNumber, 10)
	err := m.Channel.Publish(
		exchange,
		route,
		mandatory,
		immediate,
		props,
	)
	if err != nil {
		zap.L().Sugar().Errorf("failed to publish,err:%v", err)
		return
	}
	zap.L().Sugar().Infof("publish exchange:%v route:%v,call:%v", exchange, route, printCallerName())
}

func printCallerName() string {
	pc, _, _, _ := runtime.Caller(2)
	return runtime.FuncForPC(pc).Name()
}

func (m *Channel) Consumer(exchange, queue string, routes []string, exclusive, auto_ack bool, handler func(amqp.Delivery, []byte)) {
	if m.ChannelIsEmpty() || m.Channel == nil {
		zap.L().Sugar().Errorf("channel on consume is empty")
		return
	}
	if m.Connection.ConnlIsEmpty() {
		zap.L().Sugar().Errorf("connection is not ready")
		return
	}
	q, err := m.Channel.QueueDeclare(
		queue,     // name
		true,      // durable
		true,      // delete when unused
		exclusive, // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		zap.L().Sugar().Errorf("failed to declare queue,err:%v", err)
		return
	}
	for _, route := range routes {
		m.Channel.QueueBind(q.Name, route, exchange, true, nil)
	}
	msgs, err := m.Channel.Consume(queue, "", auto_ack, exclusive, false, false, nil)
	if err != nil {
		zap.L().Sugar().Errorf("failed to declare queue,err:%v", err)
		return
	}
	zap.L().Sugar().Debugf("consume exchange:%v route:%v queue:%v,call:%v", exchange, routes, q.Name, printCallerName())
	go func() {
		for msg := range msgs {
			zap.L().Sugar().Infof("consume reply to queue:%v %v", msg.ReplyTo, msg)
			if !auto_ack {
				err = msg.Ack(true)
				if err != nil {
					zap.L().Sugar().Errorf("failed to ack  %v", msg)
				}
			}
			handler(msg, msg.Body)
		}
	}()
}

func (m Channel) ChannelIsEmpty() bool {
	return reflect.DeepEqual(m.Channel, amqp.Channel{})
}
func (c *Connection) ConnlIsEmpty() bool {
	return reflect.DeepEqual(c.Mc, amqp.Connection{})
}

func (m *Channel) onChannelOpen() {
	zap.L().Sugar().Debugf("Channel %v is opening", m.Name)
	ch, err := m.Connection.Mc.Channel()

	if err != nil {
		zap.L().Sugar().Errorf("faield to create rabbitmq channel,err:%v", err)
		return
	}
	zap.L().Sugar().Debugf("initial rabbitmq channel successfully")
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		zap.L().Sugar().Errorf("failed set channel qos,err:%v", err)
	}

	zap.L().Sugar().Debugf("set channel qos successfully")
	m.Channel = ch
	if err := m.Channel.Confirm(false); err != nil {
		zap.L().Sugar().Errorf("failed to put channel in confirmation mode,err:%v", err)
		return
	}
	confirms := m.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	returned := m.Channel.NotifyReturn(make(chan amqp.Return, 1))
	go m.confirmHandler(confirms, returned)
	zap.L().Sugar().Debugf("open channel %v successfully", m.Name)

}
func (m *Channel) confirmHandler(confirms chan amqp.Confirmation, returns chan amqp.Return) {
	for {
		select {
		case confirmed := <-confirms:
			if confirmed.DeliveryTag > 0 {
				sequenceNumber := confirmed.DeliveryTag
				if confirmed.Ack {
					zap.L().Sugar().Infof("confirmed delivery with delivery tag: %d", sequenceNumber)
					if callback, ok := m.UnconfirmedMsg[sequenceNumber]; ok {
						delete(m.UnconfirmedMsg, sequenceNumber)
						if callback != nil {
							callback(nil)
						}
					}
				} else {
					for i := m.ConfirmedUpTo + 1; i < sequenceNumber+1; i++ {
						if callback, ok := m.UnconfirmedMsg[i]; ok {
							zap.L().Sugar().Errorf("rabbitmq nack message(%v), something wrong", sequenceNumber)
							callback(RabbitmqError())
						}
					}
				}
				delete(m.UnconfirmedMsg, sequenceNumber)
				m.ConfirmedUpTo = sequenceNumber
			}
		case returned := <-returns:
			number, _ := strconv.ParseUint(returned.MessageId, 10, 64)
			zap.L().Sugar().Errorf("cannot find routing, message(%v) returned", returned)
			if callback, ok := m.UnconfirmedMsg[number]; ok {
				delete(m.UnconfirmedMsg, number)
				callback(NotFound())
			}
		case <-m.Connection.NotifyClose:
			zap.L().Sugar().Errorf("rabbitmq connection closed")
			os.Exit(500)
		}
		if len(m.UnconfirmedMsg) > 1 {
			zap.L().Sugar().Errorf("outstanding confirmations: %d", len(m.UnconfirmedMsg))
		}
	}
}
func (m *Channel) Open() {
	if m.Connection.Mc == nil {
		zap.L().Sugar().Infof("please check rabbitmq status")
		return
	}
	if m.Connection.Mc.IsClosed() {
		zap.L().Sugar().Infof("get disconnect rabbitmq")
		return
	}
	if m.Channel != nil && !m.ChannelIsEmpty() {
		zap.L().Sugar().Infof("Channel %v is opening", m.Name)
		return
	}
	m.onChannelOpen()
}

func (m *Channel) Close() {
	m.Channel.Close()
	delete(m.Connection.Chs, m.Name)
}

func NewConnection(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		zap.L().Sugar().Errorf("faield to connect rabbitmq,err:%v", err)
	}

	zap.L().Sugar().Infof("connect rabbitmq successfully")
	return &Connection{Url: url, Mc: conn, Chs: make(map[string]*Channel), Quit: make(chan bool, 1)}, nil
}

func (m *Connection) channel(qos int) (*Channel, error) {
	id := uuid.New().String()
	channel := &Channel{
		Name:       id,
		Channel:    nil,
		Qos:        qos,
		Connection: m,
	}
	channel.Open()
	m.Chs[id] = channel
	zap.L().Sugar().Debugf("get connection channels:%v", m.Chs)
	return channel, nil
}

// Stop
func (m *Connection) Stop() {
	for _, ch := range m.Chs {
		ch.Close()
	}
	zap.L().Sugar().Info("Stopping")
	m.Quit <- true
}

func (m *Connection) Connect() (func(), error) {
	var err error
	zap.L().Sugar().Debugf("Connect to %v", m.Url)
	m.Mc, err = amqp.Dial(m.Url)
	if err != nil {
		zap.L().Sugar().Errorf("failed to connect to %v", m.Url)
		return nil, err
	}
	m.NotifyClose = m.Mc.NotifyClose(make(chan *amqp.Error))
	return func() {
		for _, channel := range m.Chs {
			channel.Open()
		}
	}, nil
}

// Run
func (m *Connection) Run() {
	init, err := m.Connect()
	if err != nil {
		zap.L().Sugar().Errorf("failed to connect to %v", m.Url)
		return
	}
	init()
	<-m.Quit
	m.Mc.Close()
	zap.L().Sugar().Info("close mq connection")
}
