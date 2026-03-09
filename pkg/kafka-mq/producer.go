package kafka_mq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/v2/internal/retry"
)

var ErrProducerNotReady = errors.New("producer is not ready")

type ProducerConfig struct {
	Brokers []string
	Retry   RetryConfig
	Sarama  *sarama.Config
}

type ProducerOption func(*ProducerConfig) error

func WithProducerRetry(maxAttempts int, initialBackoff, maxBackoff time.Duration) ProducerOption {
	return func(cfg *ProducerConfig) error {
		retryCfg, err := makeRetryConfig(maxAttempts, initialBackoff, maxBackoff)
		if err != nil {
			return err
		}
		cfg.Retry = retryCfg
		return nil
	}
}

func WithProducerKafkaVersion(version string) ProducerOption {
	return func(cfg *ProducerConfig) error {
		return applyKafkaVersion(cfg.Sarama, version)
	}
}

func WithProducerTLS() ProducerOption {
	return func(cfg *ProducerConfig) error {
		applyTLS(cfg.Sarama)
		return nil
	}
}

func WithProducerTLSConfig(tlsCfg *tls.Config) ProducerOption {
	return func(cfg *ProducerConfig) error {
		return applyTLSConfig(cfg.Sarama, tlsCfg)
	}
}

func WithProducerSASLPlain(username, password string) ProducerOption {
	return func(cfg *ProducerConfig) error {
		return applySASLPlain(cfg.Sarama, username, password)
	}
}

func WithProducerSASLSCRAMSHA512(username, password string) ProducerOption {
	return func(cfg *ProducerConfig) error {
		return applySASLSCRAMSHA512(cfg.Sarama, username, password)
	}
}

type Producer struct {
	cfg            *ProducerConfig
	saramaProducer sarama.SyncProducer
	mu             sync.Mutex
}

func NewProducer(brokers []string, opts ...ProducerOption) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("brokers is empty")
	}

	cfg := &ProducerConfig{
		Brokers: brokers,
		Retry: RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: 1 * time.Second,
			MaxBackoff:     10 * time.Second,
		},
		Sarama: defaultSaramaProducerConfig(),
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

	if err := validateProducerConfig(cfg); err != nil {
		return nil, err
	}

	return &Producer{
		cfg: cfg,
	}, nil
}

func defaultSaramaProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	return cfg
}

func validateProducerConfig(cfg *ProducerConfig) error {
	if len(cfg.Brokers) == 0 {
		return errors.New("brokers is empty")
	}

	if err := validateRetryConfig(cfg.Retry); err != nil {
		return err
	}

	return validateSASLConfig(cfg.Sarama)
}

func (p *Producer) Publish(topic string, data []byte) error {
	p.mu.Lock()
	producer := p.saramaProducer
	p.mu.Unlock()

	if producer == nil {
		return ErrProducerNotReady
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	if _, _, err := producer.SendMessage(msg); err != nil {
		return fmt.Errorf("failed to send message to kafka: %v", err)
	}

	return nil
}

func (p *Producer) Run(ctx context.Context) error {
	producer, err := retry.WithExpSleep(p.cfg.Retry.MaxAttempts, p.cfg.Retry.InitialBackoff, p.cfg.Retry.MaxBackoff, func() (sarama.SyncProducer, error) {
		producer, err := sarama.NewSyncProducer(p.cfg.Brokers, p.cfg.Sarama)
		if err != nil {
			return nil, err
		}
		return producer, nil
	})
	if err != nil {
		return fmt.Errorf("failed to create kafka producer: %v", err)
	}

	p.mu.Lock()
	p.saramaProducer = producer
	p.mu.Unlock()

	go func() {
		<-ctx.Done()
		p.mu.Lock()
		defer p.mu.Unlock()
		_ = producer.Close()
		p.saramaProducer = nil
	}()

	return nil
}
