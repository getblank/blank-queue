package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/getblank/blank-queue/intranet"
	"github.com/getblank/blank-queue/queue"
)

func main() {
	log.SetLevel(log.DebugLevel)
	var srAddress *string
	var port *string
	var dbFile *string
	rootCmd := &cobra.Command{
		Use:   "blank-queue",
		Short: "Queue microservice for Blank platform",
		Long:  "The next generation of web applications. This is the queue subsystem.",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("blank-queue started")
			go queue.Init(*dbFile)
			intranet.Init(*srAddress, *port)
		},
	}

	srAddress = rootCmd.PersistentFlags().StringP("service-registry", "s", "ws://localhost:1234", "Service registry uri")
	port = rootCmd.PersistentFlags().StringP("port", "p", "8083", "TCP port to listen")
	dbFile = rootCmd.PersistentFlags().StringP("db", "d", "queue.db", "Queue database filename")

	if err := rootCmd.Execute(); err != nil {
		println(err.Error())
		os.Exit(-1)
	}
}
