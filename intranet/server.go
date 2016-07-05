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
	log.WithField("args", args).Debug("Push request arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := queue.Push(q, args[1])
	if err != nil {
		log.WithError(err).Debug("Can't push item")
	}
	return nil, err
}

// args: queue string
func shiftHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Shift request arrived")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	res, err := queue.Shift(q)
	if err != nil {
		log.WithError(err).Debug("Can't shift item")
	}
	return res, err
}

// args: queue string, data interface{},
func unshiftHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Unshift request arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := queue.Unshift(q, args[1])
	if err != nil {
		log.WithError(err).Debug("Can't unshift item")
	}
	return nil, err
}

// args: queue, id string
func removeHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Remove request arrived ")
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
	if err != nil {
		log.WithError(err).WithField("_id", id).Debug("Can't remove item")
	}
	return nil, err
}

// args: queue string
func lengthHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Length request arrived ")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	l := queue.Len(q)
	log.WithField("lenght", l).WithField("queue", q).Debug("Length request processed")
	return l, nil
}

// args: queue string
func dropHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Drop request arrived ")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	q, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := queue.Drop(q)
	return nil, err
}

// args: queue, id string
func getHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Get request arrived ")
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
	data, err := queue.Get(q, id)
	if err != nil {
		log.WithError(err).WithField("_id", id).Debug("Can't get item")
	}
	return data, err
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

	wampServer.RegisterRPCHandler("queue.push", pushHandler)
	wampServer.RegisterRPCHandler("queue.shift", shiftHandler)
	wampServer.RegisterRPCHandler("queue.unshift", unshiftHandler)
	wampServer.RegisterRPCHandler("queue.remove", removeHandler)
	wampServer.RegisterRPCHandler("queue.length", lengthHandler)
	wampServer.RegisterRPCHandler("queue.drop", dropHandler)
	wampServer.RegisterRPCHandler("queue.get", getHandler)

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
