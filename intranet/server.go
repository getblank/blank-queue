package intranet

import (
	"errors"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/getblank/wango"
	"golang.org/x/net/websocket"

	"github.com/getblank/blank-queue/queue"
)

var (
	wampServer          = wango.New()
	errInvalidArguments = errors.New("invalid arguments")
)

// args: queue string, data interface{},
func pushHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := queue.Push(q, args[1])
	return nil, err
}

// args: queue string
func unshiftHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	return queue.Unshift(q)
}

// args: queue, id string
func removeHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	id, ok := args[1].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := queue.Remove(q, id)
	return nil, err
}

// args: queue string
func lengthHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	return queue.Length(q), nil
}

func internalOpenCallback(c *wango.Conn) {
	log.Info("Connected client", c.ID())
}

func internalCloseCallback(c *wango.Conn) {
	log.Info("Disconnected client", c.ID())
}

func startServer() {
	wampServer.SetSessionOpenCallback(internalOpenCallback)
	wampServer.SetSessionCloseCallback(internalCloseCallback)

	wampServer.RegisterRPCHandler("push", pushHandler)
	wampServer.RegisterRPCHandler("unshift", unshiftHandler)
	wampServer.RegisterRPCHandler("remove", removeHandler)
	wampServer.RegisterRPCHandler("length", lengthHandler)

	s := new(websocket.Server)
	s.Handshake = func(c *websocket.Config, r *http.Request) error {
		return nil
	}
	s.Handler = func(ws *websocket.Conn) {
		wampServer.WampHandler(ws, nil)
	}
	http.Handle("/", s)
	log.Info("Will listen for connection on port ", wsPort)
	err := http.ListenAndServe(":"+wsPort, nil)
	if err != nil {
		panic("ListenAndServe: " + err.Error())
	}
}
