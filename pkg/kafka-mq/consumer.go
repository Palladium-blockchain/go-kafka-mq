package kafka_mq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/retry"
)

type ConsumerConfig struct {
	ConsumerGroup string
	Brokers       []string
	Retry         RetryConfig
	Sarama        *sarama.Config
}

type ConsumerOption func(*ConsumerConfig) error

type Consumer struct {
	cfg     *ConsumerConfig
	errChan chan error
	router  *Router
}

func NewConsumer(consumerGroup string, brokers []string, router *Router, opts ...ConsumerOption) (*Consumer, error) {
	if consumerGroup == "" {
		return nil, errors.New("consumer group is empty")
	}
	if len(brokers) == 0 {
		return nil, errors.New("brokers is empty")
	}
	if router == nil {
		return nil, errors.New("router is nil")
	}

	cfg := &ConsumerConfig{
		ConsumerGroup: consumerGroup,
		Brokers:       brokers,
		Retry: RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: 1 * time.Second,
			MaxBackoff:     10 * time.Second,
		},
		Sarama: defaultSaramaConsumerConfig(),
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.Sarama == nil {
		return nil, errors.New("sarama config is nil")
	}

	if err := validateConsumerConfig(cfg); err != nil {
		return nil, err
	}

	return &Consumer{
		cfg:     cfg,
		errChan: make(chan error, 1),
		router:  router,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	// Create consumer group
	consumerGroup, err := retry.WithExpSleep(c.cfg.Retry.MaxAttempts, c.cfg.Retry.InitialBackoff, c.cfg.Retry.MaxBackoff, func() (sarama.ConsumerGroup, error) {
		consumerGroup, err := sarama.NewConsumerGroup(c.cfg.Brokers, c.cfg.ConsumerGroup, c.cfg.Sarama)
		if err != nil {
			return nil, err
		}
		return consumerGroup, nil
	})
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer group: %v", err)
	}
	defer func() { _ = consumerGroup.Close() }()

	// Run consumer
	if len(c.router.Topics()) > 0 {
		go c.runConsumer(ctx, consumerGroup)
	}

	// Lock until stop signal received or error occurred
	select {
	case <-ctx.Done():
		break
	case err := <-c.errChan:
		return fmt.Errorf("error occurred while consuming messages: %v", err)
	}

	return nil
}

func (c *Consumer) runConsumer(ctx context.Context, consumerGroup sarama.ConsumerGroup) {
	for {
		if err := consumerGroup.Consume(ctx, c.router.Topics(), c.router); err != nil {
			c.errChan <- err
			return
		}

		if ctx.Err() != nil {
			return
		}
	}
}

func WithConsumerRetry(maxAttempts int, initialBackoff, maxBackoff time.Duration) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		retryCfg, err := makeRetryConfig(maxAttempts, initialBackoff, maxBackoff)
		if err != nil {
			return err
		}
		cfg.Retry = retryCfg
		return nil
	}
}

func WithConsumerKafkaVersion(version string) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		return applyKafkaVersion(cfg.Sarama, version)
	}
}

func WithConsumerTLS() ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		applyTLS(cfg.Sarama)
		return nil
	}
}

func WithConsumerTLSConfig(tlsCfg *tls.Config) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		return applyTLSConfig(cfg.Sarama, tlsCfg)
	}
}

func WithConsumerSASLPlain(username, password string) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		return applySASLPlain(cfg.Sarama, username, password)
	}
}

func WithConsumerSASLSCRAMSHA512(username, password string) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		return applySASLSCRAMSHA512(cfg.Sarama, username, password)
	}
}

func defaultSaramaConsumerConfig() *sarama.Config {
	cfg := sarama.NewConfig()

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}

	return cfg
}

func validateConsumerConfig(cfg *ConsumerConfig) error {
	if cfg.ConsumerGroup == "" {
		return errors.New("consumer group is empty")
	}
	if len(cfg.Brokers) == 0 {
		return errors.New("brokers is empty")
	}

	if err := validateRetryConfig(cfg.Retry); err != nil {
		return err
	}

	return validateSASLConfig(cfg.Sarama)
}
