package service

import "fmt"

type Err struct {
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}

func (e Err) Error() string {
	return fmt.Sprintf("code:%v,msg:%v", e.Code, e.Msg)
}

func New(msg string) *Err {
	return &Err{
		Code: 400,
		Msg:  msg,
	}
}

func RatError() *Err {
	return &Err{
		Code: 500,
		Msg:  "Unknown Rat Error",
	}
}
func TimeOutError() *Err {
	return &Err{
		Code: 504,
		Msg:  "Timeout Error",
	}
}

func BadRequest() *Err {
	return &Err{
		Code: 400,
		Msg:  "Bad Request",
	}
}

func NotFound() *Err {
	return &Err{
		Code: 404,
		Msg:  "Not Found",
	}
}

func RabbitmqError() *Err {
	return &Err{
		Code: 502,
		Msg:  "Rabbitmq Error",
	}
}

func MongodbError() *Err {
	return &Err{
		Code: 502,
		Msg:  "Mongodb Error",
	}
}
