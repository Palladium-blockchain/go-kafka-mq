package kafka_mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/retry"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/security"
)

type ConsumerConfig struct {
	ConsumerGroup       string
	Brokers             []string
	RetryMaxAttempts    int
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	User                string
	Password            string
	CACertPath          string
	KafkaVersion        string
}

type Consumer struct {
	cfg          *ConsumerConfig
	errChan      chan error
	saramaConfig *sarama.Config
	router       *Router
}

func NewConsumer(cfg *ConsumerConfig, router *Router) (*Consumer, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}

	saramaConfig := sarama.NewConfig()

	if cfg.KafkaVersion != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(cfg.KafkaVersion)
		if err != nil {
			return nil, fmt.Errorf("parse kafka version failed: %v", err)
		}
		saramaConfig.Version = kafkaVersion
	}

	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	// Auth
	if cfg.User != "" && cfg.Password != "" {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.User
		saramaConfig.Net.SASL.Password = cfg.Password
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	// TLS
	if cfg.CACertPath != "" {
		tlsConfig, err := security.LoadTLSConfig(cfg.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("load tls config failed: %v", err)
		}

		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
	}

	return &Consumer{
		cfg:          cfg,
		errChan:      make(chan error, 1),
		saramaConfig: saramaConfig,
		router:       router,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	// Create consumer group
	consumerGroup, err := retry.WithExpSleep(c.cfg.RetryMaxAttempts, c.cfg.RetryInitialBackoff, c.cfg.RetryMaxBackoff, func() (sarama.ConsumerGroup, error) {
		consumerGroup, err := sarama.NewConsumerGroup(c.cfg.Brokers, c.cfg.ConsumerGroup, c.saramaConfig)
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
