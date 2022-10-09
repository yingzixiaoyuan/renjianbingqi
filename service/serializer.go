package service

type JsonSerializer interface {
	load_result(data []byte) interface{}
}
