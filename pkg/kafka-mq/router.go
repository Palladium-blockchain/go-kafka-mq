package kafka_mq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-mapx/pkg/mapx"
)

type HandlerFunc func(ctx context.Context, topic string, payload []byte) error

type Logger interface {
	Error(msg string, args ...any)
}

type Router struct {
	handlersMap map[string][]HandlerFunc
	ready       chan struct{}
	logger      Logger
}

type Option func(*Router)

func WithLogger(logger Logger) Option {
	return func(r *Router) {
		r.logger = logger
	}
}

func NewRouter(opts ...Option) *Router {
	r := &Router{
		handlersMap: make(map[string][]HandlerFunc),
		ready:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

func (r *Router) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (r *Router) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (r *Router) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if err := r.processMessage(session, message); err != nil {
			if r.logger != nil {
				r.logger.Error("Error processing message", "err", err)
			} else {
				log.Printf("Error processing message: %v", err)
			}
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func (r *Router) RegisterHandler(topic string, handler HandlerFunc) error {
	handlersSlice, ok := r.handlersMap[topic]
	if !ok || handlersSlice == nil {
		handlersSlice = []HandlerFunc{}
	}

	r.handlersMap[topic] = append(handlersSlice, handler)

	return nil
}

func (r *Router) Topics() []string {
	topics := mapx.Keys(r.handlersMap)
	slices.Sort(topics)
	return topics
}

func (r *Router) processMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	handlerSlice, ok := r.handlersMap[msg.Topic]
	if !ok {
		return errors.New("topic not found")
	}

	for _, handler := range handlerSlice {
		if err := handler(session.Context(), msg.Topic, msg.Value); err != nil {
			return fmt.Errorf("message handled with error: %v", err)
		}
	}

	return nil
}
