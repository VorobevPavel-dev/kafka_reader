package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/VorobevPavel-dev/kafka_reader/consumer"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "c", "./config.toml", "Path to config file")
}

func main() {
	flag.Parse()
	var settings Settings
	settings = ParseConfig(configPath)

	//Connecting to log file
	file, err := os.OpenFile(settings.LoggingSettings.LogTarget, os.O_WRONLY, 0666)
	if err != nil {
		log.Errorf("Error while connecting to log file: %v", err)
		os.Exit(3)
	}
	defer file.Close()

	//Determine log level
	switch settings.LoggingSettings.LogLevel {
	case "panic":
		log.SetLevel(log.PanicLevel)
		break
	case "fatal":
		log.SetLevel(log.FatalLevel)
		break
	case "error":
		log.SetLevel(log.ErrorLevel)
		break
	case "warn":
		log.SetLevel(log.WarnLevel)
		break
	case "info":
		log.SetLevel(log.InfoLevel)
		break
	case "debug":
		log.SetLevel(log.DebugLevel)
		break
	case "trace":
		log.SetLevel(log.TraceLevel)
		break
	}

	var done = make(chan struct{})
	defer close(done)
	consumer, err := consumer.StartBatchConsumer(
		settings.ConnectionSettings.Brokers,
		settings.ConnectionSettings.Topic,
		settings.BatchSettings.MaxBufferSize,
		settings.ConnectionSettings.Version,
		settings.ConnectionSettings.Group,
		settings.BatchSettings.BufferCapacity,
		settings.BatchSettings.TickerIntervalSeconds,
		settings.OutputSettings.Target,
		settings.OutputSettings.TargetAddress,
	)
	if err != nil {
		log.Printf("[main.go][main] Fatal error occured: %v", err)
		panic(err)
	}
	defer consumer.Close()
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	log.Println("Recieved signal", <-c)

}
