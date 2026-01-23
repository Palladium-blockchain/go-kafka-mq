package kafka_mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/retry"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/security"
)

var ErrProducerNotReady = errors.New("producer is not ready")

type ProducerConfig struct {
	Brokers             []string
	RetryMaxAttempts    int
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	User                string
	Password            string
	CACertPath          string
	KafkaVersion        string
}

type Producer struct {
	cfg            *ProducerConfig
	saramaConfig   *sarama.Config
	saramaProducer sarama.SyncProducer
	mu             sync.Mutex
}

func NewProducer(cfg *ProducerConfig) (*Producer, error) {
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

	saramaConfig.Producer.Return.Successes = true

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

	return &Producer{
		cfg:            cfg,
		saramaConfig:   saramaConfig,
		saramaProducer: nil,
	}, nil
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
	producer, err := retry.WithExpSleep(p.cfg.RetryMaxAttempts, p.cfg.RetryInitialBackoff, p.cfg.RetryMaxBackoff, func() (sarama.SyncProducer, error) {
		producer, err := sarama.NewSyncProducer(p.cfg.Brokers, p.saramaConfig)
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
