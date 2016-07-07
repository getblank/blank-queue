package intranet

import (
	"errors"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/getblank/wango"
	"golang.org/x/net/websocket"

	"github.com/getblank/blank-queue/lists"
	"github.com/getblank/blank-queue/queue"
)

var (
	wampServer          = wango.New()
	errInvalidArguments = errors.New("invalid arguments")
)

// args: queue string, data interface{},
func queuePushHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
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
func queueShiftHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
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
func queueUnshiftHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
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
func queueRemoveHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Remove request arrived")
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
func queueLengthHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Length request arrived")
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
func queueDropHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Drop request arrived")
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
func queueGetHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("Get request arrived")
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

type m map[string]interface{}

// args: list string
func listFrontHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List front request arrived")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	e, n, err := lists.Front(l)
	if err != nil {
		log.WithError(err).Debug("Can't get element")
		return nil, err
	}
	return m{"element": e, "position": n}, err
}

// args: list string
func listBackHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Back arrived")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	e, n, err := lists.Back(l)
	if err != nil {
		log.WithError(err).Debug("Can't get element")
		return nil, err
	}
	return m{"element": e, "position": n}, err
}

// args: list string, element interface{}
func listPushBackHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List PushBack arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	n, err := lists.PushBack(l, args[1])
	if err != nil {
		log.WithError(err).Debug("Can't push element to back")
		return nil, err
	}
	return n, err
}

// args: list string, element interface{}
func listPushFrontHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List PushFront arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	n, err := lists.PushFront(l, args[1])
	if err != nil {
		log.WithError(err).Debug("Can't push element to front")
		return nil, err
	}
	return n, err
}

// args: list string, n float64
func listNextHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Next arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	_n, ok := args[1].(float64)
	if !ok {
		return nil, errInvalidArguments
	}
	e, n, err := lists.Next(l, int(_n))
	if err != nil {
		log.WithError(err).WithField("position", _n).Debug("Can't get next element")
		return nil, err
	}
	return m{"element": e, "position": n}, err
}

// args: list string, n float64
func listPrevHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Prev arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	_n, ok := args[1].(float64)
	if !ok {
		return nil, errInvalidArguments
	}
	e, n, err := lists.Prev(l, int(_n))
	if err != nil {
		log.WithError(err).WithField("position", _n).Debug("Can't get prev element")
		return nil, err
	}
	return m{"element": e, "position": n}, err
}

// args: list string, n float64
func listRemoveHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Remove arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	_n, ok := args[1].(float64)
	if !ok {
		return nil, errInvalidArguments
	}
	err := lists.Remove(l, int(_n))
	if err != nil {
		log.WithError(err).WithField("position", _n).Debug("Can't remove element")
	}
	return nil, err
}

// args: list string, _id string
func listRemoveByIDHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List RemoveByID arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	_id, ok := args[1].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := lists.RemoveByID(l, _id)
	if err != nil {
		log.WithError(err).WithField("_id", _id).Debug("Can't remove element")
	}
	return nil, err
}

// args: list string
func listDropHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Drop arrived")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := lists.Drop(l)
	if err != nil {
		log.WithError(err).Debug("Can't drop list")
	}
	return nil, err
}

// args: list string, n float64
func listGetHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Get arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	_n, ok := args[1].(float64)
	if !ok {
		return nil, errInvalidArguments
	}
	e, err := lists.Get(l, int(_n))
	if err != nil {
		log.WithError(err).WithField("position", _n).Debug("Can't get element")
	}
	return e, err
}

// args: list string, _id string
func listGetByIDHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List GetByID arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	_id, ok := args[1].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	e, n, err := lists.GetByID(l, _id)
	if err != nil {
		log.WithError(err).WithField("_id", _id).Debug("Can't get element by _id")
	}
	return m{"element": e, "position": n}, err
}

// args: list string, data interface{}
func listUpdateByIDHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List UpdateByID arrived")
	if len(args) < 2 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	err := lists.UpdateByID(l, args[1])
	if err != nil {
		log.WithError(err).Debug("Can't update element by _id")
	}
	return nil, err
}

// args: list string
func listLengthHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	log.WithField("args", args).Debug("List Length arrived")
	if len(args) == 0 {
		return nil, errInvalidArguments
	}
	l, ok := args[0].(string)
	if !ok {
		return nil, errInvalidArguments
	}
	length := lists.Len(l)
	return length, nil
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

	wampServer.RegisterRPCHandler("queue.push", queuePushHandler)
	wampServer.RegisterRPCHandler("queue.shift", queueShiftHandler)
	wampServer.RegisterRPCHandler("queue.unshift", queueUnshiftHandler)
	wampServer.RegisterRPCHandler("queue.remove", queueRemoveHandler)
	wampServer.RegisterRPCHandler("queue.length", queueLengthHandler)
	wampServer.RegisterRPCHandler("queue.drop", queueDropHandler)
	wampServer.RegisterRPCHandler("queue.get", queueGetHandler)

	wampServer.RegisterRPCHandler("list.front", listFrontHandler)
	wampServer.RegisterRPCHandler("list.back", listBackHandler)
	wampServer.RegisterRPCHandler("list.pushBack", listPushBackHandler)
	wampServer.RegisterRPCHandler("list.pushFront", listPushFrontHandler)
	wampServer.RegisterRPCHandler("list.next", listNextHandler)
	wampServer.RegisterRPCHandler("list.prev", listPrevHandler)
	wampServer.RegisterRPCHandler("list.remove", listRemoveHandler)
	wampServer.RegisterRPCHandler("list.removeById", listRemoveByIDHandler)
	wampServer.RegisterRPCHandler("list.drop", listDropHandler)
	wampServer.RegisterRPCHandler("list.get", listGetHandler)
	wampServer.RegisterRPCHandler("list.getById", listGetByIDHandler)
	wampServer.RegisterRPCHandler("list.updateById", listUpdateByIDHandler)
	wampServer.RegisterRPCHandler("list.length", listLengthHandler)

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
