package main

import (
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
)

type Settings struct {
	BatchSettings      Batch      `toml:"Batch"`
	OutputSettings     Output     `toml:"Output"`
	LoggingSettings    Logging    `toml:"Logging"`
	ConnectionSettings Connection `toml:"Connection"`
}

type Batch struct {
	BufferCapacity        int `toml:"BufferCapacity"`
	MaxBufferSize         int `toml:"MaxBufferSize"`
	TickerIntervalSeconds int `toml:"TickerIntervalSeconds"`
}

type Connection struct {
	Brokers []string `toml:"Brokers"`
	Topic   string   `toml:"Topic"`
	Version string   `toml:"Version"`
	Group   string   `toml:"Group"`
}

type Output struct {
	Target        string `toml:"Target"`
	TargetAddress string `toml:"TargetAddress"`
}

type Logging struct {
	LogLevel  string `toml:"LogLevel"`
	LogTarget string `toml:"LogTarget"`
}

func ParseConfig(path string) Settings {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Errorf("[config.go][ParseConfig] Error while getting config: %v", err)
		os.Exit(1)
	}
	var config Settings
	_, err = toml.Decode(string(data), &config)
	if err != nil {
		log.Errorf("[config.go][ParseConfig] Error while parsing config: %v", err)
		os.Exit(1)
	}
	return config
}
