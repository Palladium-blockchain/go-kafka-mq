package kafka_mq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/retry"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/scram"
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
		if maxAttempts <= 0 {
			return errors.New("retry max attempts must be > 0")
		}
		if initialBackoff <= 0 {
			return errors.New("retry initial backoff must be > 0")
		}
		if maxBackoff <= 0 {
			return errors.New("retry max backoff must be > 0")
		}

		cfg.Retry = RetryConfig{
			MaxAttempts:    maxAttempts,
			InitialBackoff: initialBackoff,
			MaxBackoff:     maxBackoff,
		}
		return nil
	}
}

func WithConsumerKafkaVersion(version string) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		if version == "" {
			return nil
		}

		kafkaVersion, err := sarama.ParseKafkaVersion(version)
		if err != nil {
			return fmt.Errorf("parse kafka version failed: %w", err)
		}

		cfg.Sarama.Version = kafkaVersion
		return nil
	}
}

func WithConsumerTLS() ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		cfg.Sarama.Net.TLS.Enable = true
		if cfg.Sarama.Net.TLS.Config == nil {
			cfg.Sarama.Net.TLS.Config = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		} else if cfg.Sarama.Net.TLS.Config.MinVersion == 0 {
			cfg.Sarama.Net.TLS.Config.MinVersion = tls.VersionTLS12
		}
		return nil
	}
}

func WithConsumerTLSConfig(tlsCfg *tls.Config) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		if tlsCfg == nil {
			return errors.New("tls config is nil")
		}
		if tlsCfg.MinVersion == 0 {
			tlsCfg.MinVersion = tls.VersionTLS12
		}

		cfg.Sarama.Net.TLS.Enable = true
		cfg.Sarama.Net.TLS.Config = tlsCfg
		return nil
	}
}

func WithConsumerSASLPlain(username, password string) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		if username == "" {
			return errors.New("plain username is empty")
		}
		if password == "" {
			return errors.New("plain password is empty")
		}

		cfg.Sarama.Net.SASL.Enable = true
		cfg.Sarama.Net.SASL.User = username
		cfg.Sarama.Net.SASL.Password = password
		cfg.Sarama.Net.SASL.Handshake = true
		cfg.Sarama.Net.SASL.Mechanism = sarama.SASLTypePlaintext

		return nil
	}
}

func WithConsumerSASLSCRAMSHA512(username, password string) ConsumerOption {
	return func(cfg *ConsumerConfig) error {
		if username == "" {
			return errors.New("scram username is empty")
		}
		if password == "" {
			return errors.New("scram password is empty")
		}

		cfg.Sarama.Net.SASL.Enable = true
		cfg.Sarama.Net.SASL.User = username
		cfg.Sarama.Net.SASL.Password = password
		cfg.Sarama.Net.SASL.Handshake = true
		cfg.Sarama.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Sarama.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
		}

		return nil
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
	if cfg.Retry.MaxAttempts <= 0 {
		return errors.New("retry max attempts must be > 0")
	}
	if cfg.Retry.InitialBackoff <= 0 {
		return errors.New("retry initial backoff must be > 0")
	}
	if cfg.Retry.MaxBackoff <= 0 {
		return errors.New("retry max backoff must be > 0")
	}

	sc := cfg.Sarama

	if sc.Net.SASL.Enable {
		if sc.Net.SASL.User == "" {
			return errors.New("sasl user is empty")
		}
		if sc.Net.SASL.Password == "" {
			return errors.New("sasl password is empty")
		}

		switch sc.Net.SASL.Mechanism {
		case sarama.SASLTypePlaintext:
		case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512:
			if sc.Net.SASL.SCRAMClientGeneratorFunc == nil {
				return errors.New("scram client generator is not configured")
			}
		default:
			return fmt.Errorf("unsupported sasl mechanism: %q", sc.Net.SASL.Mechanism)
		}
	}

	return nil
}
