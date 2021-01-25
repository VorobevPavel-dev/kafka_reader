package consumer

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Shopify/sarama"
)

//BatchConfig is a part of global config
type BatchConfig struct {
	BufferCapacity        int
	MaxBufferSize         int
	TickerIntervalSeconds int
	Callback              func([]*SessionMessage) error
	OutStream             io.Writer
	Output                OutputSettings
}

type OutputSettings struct {
	Target        string
	TargetAddress string
}

type batchConsumerHandler struct {
	cfg   *BatchConfig
	ready chan bool

	ticker *time.Ticker
	msgBuf []*SessionMessage

	mu sync.RWMutex

	cb func([]*SessionMessage) error
}

func NewBatchGroupHandler(cfg *BatchConfig, bufferCapacity int, tickerIntervalSeconds int) GroupHandler {
	handler := batchConsumerHandler{
		ready: make(chan bool, 0),
		cb:    cfg.Callback,
	}

	if cfg.BufferCapacity == 0 {
		log.Println("[batch_consumer.go][NewBatchGroupHandler] BufferCapacity was not specified. Set by default on 10000")
		cfg.BufferCapacity = 10000
	}

	if cfg.TickerIntervalSeconds == 0 {
		log.Println("[batch_consumer.go][NewBatchGroupHandler] TickerIntervalSeconds was not specified. Set by default on 1min")
		cfg.TickerIntervalSeconds = 60
	}

	handler.cfg = cfg
	handler.ticker = time.NewTicker(time.Duration(cfg.TickerIntervalSeconds) * time.Second)

	var err error
	handler.cfg.OutStream, err = getTarget(cfg)
	if err != nil {
		log.Error("[batch_consumer.go][NewBatchGroupHandler] Cannot find out target")
		os.Exit(2)
	}

	return &handler
}

//getTarget opens file or connection to send data
func getTarget(cfg *BatchConfig) (io.Writer, error) {
	switch cfg.Output.Target {
	case "file":
		file, err := os.OpenFile(cfg.Output.TargetAddress, os.O_WRONLY, 0666)
		if err != nil {
			log.Errorf("[batch_consumer.go][getTarget] An error occured while targeting output: %v", err)
			os.Exit(1)
		}
		//TODO: defer file.close()
		return file, err
	case "host":
		socket, err := net.Dial("tcp", cfg.Output.TargetAddress)
		if err != nil {
			log.Errorf("[batch_consumer.go][getTarget] An error occured while targeting output: %v", err)
			os.Exit(1)
		}
		cfg.OutStream = socket
		return socket, err
	}
	return nil, errors.New("Incorrect target was specified")
}

func (h *batchConsumerHandler) appendMessage(msg *SessionMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgBuf = append(h.msgBuf, msg)
	if len(h.msgBuf) >= h.cfg.MaxBufferSize {
		h.flushBuffer()
	}
}

//Just flushing and nothing more
func (h *batchConsumerHandler) flushBuffer() {
	if len(h.msgBuf) > 0 {
		if err := h.cb(h.msgBuf); err == nil {
			for _, msg := range h.msgBuf {
				io.WriteString(h.cfg.OutStream, string(msg.Message.Value))
			}
			h.msgBuf = make([]*SessionMessage, 0, h.cfg.BufferCapacity)
		}
	}
}

func (h *batchConsumerHandler) WaitReady() {
	<-h.ready
	return
}

func (h *batchConsumerHandler) Reset() {
	h.ready = make(chan bool, 0)
	return
}

func (h *batchConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *batchConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	claimMsgChan := claim.Messages()

	for {
		select {
		case message, ok := <-claimMsgChan:
			if ok {
				h.appendMessage(
					&SessionMessage{
						Message: message,
						Session: session,
					})
			} else {
				return nil
			}
		case <-h.ticker.C:
			h.mu.Lock()
			h.flushBuffer()
			h.mu.Unlock()
		}
	}
}
