package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	ResultExchange  = "echo"
	RequestExchange = "wormhole"
)

type Service struct {
	Name                   string
	Connection             *Connection
	ClientChannel          *Channel
	ClientQueue            string
	ClientWaitingResponses map[string]func(interface{})
}

func NewService(name string, uri string) (*Service, error) {
	newConn, err := NewConnection(uri)
	if err != nil {
		return nil, err
	}
	return &Service{
		Name:                   name,
		Connection:             newConn,
		ClientWaitingResponses: make(map[string]func(interface{})),
	}, nil
}
func (s *Service) ensureClientChannel() {
	if (s.ClientChannel == nil || s.ClientChannel == &Channel{}) {
		ch, err := s.Connection.channel(1)
		s.ClientChannel = ch
		zap.L().Sugar().Debugf("create client channel:%v", s.ClientChannel)
		if err != nil {
			zap.L().Sugar().Errorf("failed to initial rabbitmq channel,err:%v", err)
			return
		}
	}
}

func (s *Service) clientMsgHandler(props amqp.Delivery, data []byte) {
	zap.L().Sugar().Infof("callback with props: %v, data: %s", props.CorrelationId, string(data))
	if callback, ok := s.ClientWaitingResponses[props.CorrelationId]; ok {
		delete(s.ClientWaitingResponses, props.CorrelationId)
		callback(data)
		zap.L().Sugar().Debugf("execute callback:%v function", callback)
	}
}

func (s *Service) ensureClientQueue() {
	if len(s.ClientQueue) > 0 {
		return
	}
	s.ensureClientChannel()
	s.ClientQueue = uuid.New().String()
	s.ClientChannel.Consumer(ResultExchange, s.ClientQueue, []string{s.ClientQueue}, true, true, s.clientMsgHandler)
}

func (s *Service) callSync(route string, data interface{}, out proto.Message, callback func(error, interface{})) {
	s.ensureClientQueue()
	corrId := uuid.New().String()
	b, err := json.Marshal(data)
	if err != nil {
		zap.L().Sugar().Errorf("failed to marshal data:%v to byte", data)
		return
	}
	props := amqp.Publishing{
		CorrelationId: corrId,
		ReplyTo:       s.ClientQueue,
		Body:          b,
		AppId:         s.Name,
	}
	s.ClientWaitingResponses[corrId] = func(data interface{}) {
		// 1. 序列化参数,是否失败
		switch data.(type) {
		case *Err:
			callback(data.(*Err), nil)
		case []byte:
			UnmarshalOptions := protojson.UnmarshalOptions{
				DiscardUnknown: true,
				AllowPartial:   true,
			}
			err := UnmarshalOptions.Unmarshal(data.([]byte), out)
			if err != nil {
				zap.L().Sugar().Errorf("failed to unmarshal data:%v to params:%v,err:%v", data, out, err)
				callback(err, nil)
				return
			}
			callback(nil, out)
		}
	}
	s.ClientChannel.Publish(RequestExchange, route, true, false, props, func(reason *Err) {
		if reason != nil {
			bytes, _ := json.Marshal(reason)
			s.clientMsgHandler(amqp.Delivery{CorrelationId: corrId}, bytes)
		}
	})
	zap.L().Sugar().Infof("publish exchange:%v route:%v with props: %v, data: %s", RequestExchange, route, props, data)
}

func (s *Service) Call(route string, data map[string]interface{}, out proto.Message) interface{} {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	done := make(chan interface{}, 1)
	go func() {
		var result interface{}
		s.callSync(route, data, out, func(err error, args interface{}) {
			if err != nil {
				result = err
			} else {
				result = args
			}
			done <- result
		})
	}()
	select {
	case v, ok := <-done:
		if ok {
			return v
		}
	case <-ctx.Done():
		return TimeOutError()
	}
	return nil
}

func (s *Service) Serve() {
	zap.L().Sugar().Infof("server %s started.", s.Name)
	s.Connection.Run()
}

func (s *Service) Listen(route string, extraRoutes []string, qos int, handler func(proto.Message) (proto.Message, error), in proto.Message) {
	zap.L().Sugar().Debugf("add listener: %v, %v, %v,", route, extraRoutes, qos)
	channel, err := s.Connection.channel(qos)
	if err != nil {
		zap.L().Sugar().Errorf("failed to create channel on listen,err:%v", err)
		return
	}
	routes := make([]string, 0)
	routes = append(routes, route)
	if len(extraRoutes) > 0 {
		routes = append(routes, extraRoutes...)
	}
	channel.Consumer(RequestExchange, route, routes, false, false, func(props amqp.Delivery, data []byte) {
		reply := props.ReplyTo
		correlationId := props.CorrelationId
		var bytes []byte
		UnmarshalOptions := protojson.UnmarshalOptions{
			DiscardUnknown: true,
			AllowPartial:   true,
		}
		UnmarshalOptions.Unmarshal(data, in)
		payload, funcErr := handler(in)
		if funcErr != nil {
			zap.L().Sugar().Errorf("failed to handle route:%v,err:%v", route, funcErr)
			bytes, _ = json.Marshal(funcErr)
		} else {
			MarshalOptions := protojson.MarshalOptions{
				EmitUnpopulated: true,
				UseProtoNames:   true,
			}
			bytes, _ = MarshalOptions.Marshal(payload)
		}
		channel.Publish(ResultExchange, reply, true, false, amqp.Publishing{
			CorrelationId: correlationId,
			AppId:         s.Name,
			Body:          bytes,
		}, nil)
		zap.L().Sugar().Infof("handle request end, handler:,queue:%v client: %s", route, props.AppId)
	})
}

func (s *Service) Subscribe(queue, route string, extraRoutes []string, handler func(proto.Message) (proto.Message, error), in proto.Message) {
	zap.L().Sugar().Debugf("add subscriber: %v, %v, %v,", queue, route, extraRoutes)
	channel, err := s.Connection.channel(0)
	if err != nil {
		zap.L().Sugar().Debugf("failed to create channel on subscribe,err:%v", err)
		return
	}
	routes := make([]string, 0)
	routes = append(routes, route)
	if len(extraRoutes) > 0 {
		routes = append(routes, extraRoutes...)
	}
	channel.Consumer(ResultExchange, queue, routes, false, true, func(props amqp.Delivery, data []byte) {
		UnmarshalOptions := protojson.UnmarshalOptions{
			DiscardUnknown: true,
			AllowPartial:   true,
		}
		UnmarshalOptions.Unmarshal(data, in)
		_, funcErr := handler(in)
		if funcErr != nil {
			zap.L().Sugar().Infof("handle broadcast message error:%v", funcErr)
		}
		zap.L().Sugar().Infof("handle broadcast end, handler: %v, client: %s", handler, props.AppId)
		zap.L().Sugar().Debugf("consume with props: %v, params: %s", props, data)
	})
}

func (s *Service) Publish(route string, data proto.Message) {
	s.ensureClientChannel()
	MarshalOptions := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}
	payload, _ := MarshalOptions.Marshal(data)
	zap.L().Sugar().Debugf("publish with route:  %s, payload: %s", route, payload)
	s.ClientChannel.Publish(ResultExchange, route, false, false, amqp.Publishing{
		AppId: s.Name,
		Body:  payload,
	}, nil)
}
