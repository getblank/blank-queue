package intranet

import (
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/getblank/wango"
	"golang.org/x/net/websocket"
)

var (
	wampServer = wango.New()
)

// args: queue string, data interface{},
func pushHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	return nil, nil
}

// args: queue string
func unshiftHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	return nil, nil
}

// args: queue, id string
func removeHandler(c *wango.Conn, _uri string, args ...interface{}) (interface{}, error) {
	return nil, nil
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
