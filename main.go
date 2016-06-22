package main

import (
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/getblank/blank-queue/intranet"
	"github.com/getblank/blank-queue/queue"
)

var (
	buildTime string
	version   string
	gitHash   string
)

func main() {
	log.SetLevel(log.DebugLevel)
	var srAddress *string
	var port *string
	var dbFile *string
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
			go queue.Init(*dbFile)
			intranet.Init(*srAddress, *port)
		},
	}

	srAddress = rootCmd.PersistentFlags().StringP("service-registry", "s", "ws://localhost:1234", "Service registry uri")
	port = rootCmd.PersistentFlags().StringP("port", "p", "8083", "TCP port to listen")
	dbFile = rootCmd.PersistentFlags().StringP("db", "d", "queue.db", "Queue database filename")
	verFlag = rootCmd.PersistentFlags().BoolP("version", "v", false, "Prints version and exit")

	if err := rootCmd.Execute(); err != nil {
		println(err.Error())
		os.Exit(-1)
	}
}

func printVersion() {
	fmt.Printf("Build time:  		%s\n", buildTime)
	fmt.Printf("Commit hash: 		%s\n", gitHash)
	if version == "" {
		fmt.Println("This build has no version.")
		return
	}
	fmt.Printf("Version:     		%s\n", version)
}
