package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/gemnasium/logrus-graylog-hook.v2"

	"github.com/getblank/blank-queue/intranet"
	"github.com/getblank/blank-queue/lists"
	"github.com/getblank/blank-queue/queue"
)

var (
	buildTime string
	gitHash   string
	version   = "0.1.11"
)

func main() {
	if os.Getenv("BLANK_DEBUG") != "" {
		log.SetLevel(log.DebugLevel)
	}
	log.SetFormatter(&log.JSONFormatter{})
	if os.Getenv("GRAYLOG2_HOST") != "" {
		host := os.Getenv("GRAYLOG2_HOST")
		port := os.Getenv("GRAYLOG2_PORT")
		if port == "" {
			port = "12201"
		}
		source := os.Getenv("GRAYLOG2_SOURCE")
		if source == "" {
			source = "blank-queue"
		}
		hook := graylog.NewGraylogHook(host+":"+port, map[string]interface{}{"source-app": source})
		log.AddHook(hook)
	}

	srAddress := flag.String("s", "ws://localhost:1234", "Service registry uri")
	port := flag.String("p", "8083", "TCP port to listen")
	qdbFile := flag.String("q", "queue.db", "Queue database filename")
	ldbFile := flag.String("l", "lists.db", "Lists database filename")
	verFlag := flag.Bool("v", false, "Prints version and exit")
	flag.Parse()

	if *verFlag {
		printVersion()
		return
	}

	if sr := os.Getenv("BLANK_SERVICE_REGISTRY_URI"); len(sr) > 0 {
		srAddress = &sr
	}
	if srPort := os.Getenv("BLANK_SERVICE_REGISTRY_PORT"); len(srPort) > 0 {
		addr := "ws://localhost:" + srPort
		srAddress = &addr
	}

	log.Info("blank-queue started")
	go queue.Init(*qdbFile)
	go lists.Init(*ldbFile)
	intranet.Init(*srAddress, *port)
}

func printVersion() {
	fmt.Printf("blank-queue: \tv%s \t build time: %s \t commit hash: %s \n", version, buildTime, gitHash)
}
