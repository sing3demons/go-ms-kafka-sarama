package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

// IMicroservice is interface for centralized service management
type IMicroservice interface {
	Start() error
	Stop()
	Cleanup() error
	Log(message string)

	// Consumer Services
	Consume(servers string, topic string, groupID string, h ServiceHandleFunc) error
}

// Microservice is the centralized service management
type Microservice struct {
	exitChannel chan bool
}

// IContext is the context for service
type IContext interface {
	Log(message string)
	Param(name string) string
	Response(responseCode int, responseData interface{})
	ReadInput() string
}

// ServiceHandleFunc is the handler for each Microservice
type ServiceHandleFunc func(ctx IContext) error

// NewMicroservice is the constructor function of Microservice
func NewMicroservice() *Microservice {
	return &Microservice{}
}

// Consume registers a consumer for the service
func (ms *Microservice) Consume(servers string, topic string, groupID string, h ServiceHandleFunc) error {
	go func() {
		config := sarama.NewConfig()
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.Version = sarama.V2_5_0_0 // Set to Kafka version used

		client, err := sarama.NewConsumerGroup([]string{servers}, groupID, config)
		if err != nil {
			ms.Log("Consumer", fmt.Sprintf("Error creating consumer group: %v", err))
			return
		}
		defer client.Close()

		handler := &ConsumerGroupHandler{
			ms:    ms,
			h:     h,
			topic: topic,
		}

		ctx := context.Background()
		for {
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				ms.Log("Consumer", fmt.Sprintf("Error during consume: %v", err))
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

// Start starts the microservice
func (ms *Microservice) Start() error {
	osQuit := make(chan os.Signal, 1)
	ms.exitChannel = make(chan bool, 1)
	signal.Notify(osQuit, syscall.SIGTERM, syscall.SIGINT)
	exit := false
	for {
		if exit {
			break
		}
		select {
		case <-osQuit:
			exit = true
		case <-ms.exitChannel:
			exit = true
		}
	}
	return nil
}

// Stop stops the microservice
func (ms *Microservice) Stop() {
	if ms.exitChannel == nil {
		return
	}
	ms.exitChannel <- true
}

// Cleanup performs cleanup before exit
func (ms *Microservice) Cleanup() error {
	return nil
}

// Log logs a message to the console
func (ms *Microservice) Log(tag string, message string) {
	fmt.Println(tag+":", message)
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	ms    *Microservice
	h     ServiceHandleFunc
	topic string
}

// Setup is run at the beginning of a new session
func (handler *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session
func (handler *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from a claim
func (handler *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		handler.ms.Log("Consumer", fmt.Sprintf("Message received: %s", string(msg.Value)))
		ctx := NewConsumerContext(handler.ms, string(msg.Value))
		if err := handler.h(ctx); err != nil {
			handler.ms.Log("Consumer", fmt.Sprintf("Handler error: %v", err))
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

// ConsumerContext implements IContext
type ConsumerContext struct {
	ms      *Microservice
	message string
}

// NewConsumerContext is the constructor function for ConsumerContext
func NewConsumerContext(ms *Microservice, message string) *ConsumerContext {
	return &ConsumerContext{
		ms:      ms,
		message: message,
	}
}

// Log logs a message
func (ctx *ConsumerContext) Log(message string) {
	fmt.Println("Consumer:", message)
}

// Param returns a parameter by name (not used in this example)
func (ctx *ConsumerContext) Param(name string) string {
	return ""
}

// ReadInput returns the message
func (ctx *ConsumerContext) ReadInput() string {
	return ctx.message
}

// Response returns a response to the client (not used in this example)
func (ctx *ConsumerContext) Response(responseCode int, responseData interface{}) {
	return
}
