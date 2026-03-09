package kafka_mq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/retry"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/scram"
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

func WithProducerKafkaVersion(version string) ProducerOption {
	return func(cfg *ProducerConfig) error {
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

func WithProducerTLS() ProducerOption {
	return func(cfg *ProducerConfig) error {
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

func WithProducerTLSConfig(tlsCfg *tls.Config) ProducerOption {
	return func(cfg *ProducerConfig) error {
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

func WithProducerSASLPlain(username, password string) ProducerOption {
	return func(cfg *ProducerConfig) error {
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

func WithProducerSASLSCRAMSHA512(username, password string) ProducerOption {
	return func(cfg *ProducerConfig) error {
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
