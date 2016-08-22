package main

import (
	"fmt"
	"os"

	"gopkg.in/gemnasium/logrus-graylog-hook.v2"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/getblank/blank-queue/intranet"
	"github.com/getblank/blank-queue/lists"
	"github.com/getblank/blank-queue/queue"
)

var (
	buildTime string
	gitHash   string
	version   = "0.1.6"
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

	var srAddress, port, qdbFile, ldbFile *string
	var verFlag *bool
	rootCmd := &cobra.Command{
		Use:   "blank-queue",
		Short: "Queue microservice for Blank platform",
		Long:  "The next generation of web applications. This is the queue subsystem.",
		Run: func(cmd *cobra.Command, args []string) {
			if *verFlag {
				printVersion()
				return
			}
			log.Info("blank-queue started")
			go queue.Init(*qdbFile)
			go lists.Init(*ldbFile)
			intranet.Init(*srAddress, *port)
		},
	}

	srAddress = rootCmd.PersistentFlags().StringP("service-registry", "s", "ws://localhost:1234", "Service registry uri")
	port = rootCmd.PersistentFlags().StringP("port", "p", "8083", "TCP port to listen")
	qdbFile = rootCmd.PersistentFlags().StringP("qdb", "q", "queue.db", "Queue database filename")
	ldbFile = rootCmd.PersistentFlags().StringP("ldb", "l", "lists.db", "Lists database filename")
	verFlag = rootCmd.PersistentFlags().BoolP("version", "v", false, "Prints version and exit")

	if err := rootCmd.Execute(); err != nil {
		println(err.Error())
		os.Exit(-1)
	}
}

func printVersion() {
	fmt.Printf("blank-queue: \tv%s \t build time: %s \t commit hash: %s \n", version, buildTime, gitHash)
}
