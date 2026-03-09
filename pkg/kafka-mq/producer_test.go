package kafka_mq

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type mockSyncProducer struct {
	sendErr  error
	captured *sarama.ProducerMessage
}

func (m *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	m.captured = msg
	if m.sendErr != nil {
		return 0, 0, m.sendErr
	}
	return 1, 1, nil
}

func (m *mockSyncProducer) SendMessages([]*sarama.ProducerMessage) error { return nil }
func (m *mockSyncProducer) Close() error                                 { return nil }
func (m *mockSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag      { return 0 }
func (m *mockSyncProducer) IsTransactional() bool                        { return false }
func (m *mockSyncProducer) BeginTxn() error                              { return nil }
func (m *mockSyncProducer) CommitTxn() error                             { return nil }
func (m *mockSyncProducer) AbortTxn() error                              { return nil }
func (m *mockSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (m *mockSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func TestNewProducerValidDefaultConfig(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if p == nil || p.cfg == nil || p.cfg.Sarama == nil {
		t.Fatal("expected non-nil producer config")
	}
}

func TestNewProducerEmptyBrokers(t *testing.T) {
	_, err := NewProducer(nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestWithProducerRetryValidation(t *testing.T) {
	cfg := &ProducerConfig{Sarama: defaultSaramaProducerConfig()}

	if err := WithProducerRetry(0, time.Second, time.Second)(cfg); err == nil {
		t.Fatal("expected maxAttempts validation error")
	}
	if err := WithProducerRetry(1, 0, time.Second)(cfg); err == nil {
		t.Fatal("expected initialBackoff validation error")
	}
	if err := WithProducerRetry(1, time.Second, 0)(cfg); err == nil {
		t.Fatal("expected maxBackoff validation error")
	}
}

func TestWithProducerTLSConfigSetsMinVersion(t *testing.T) {
	cfg := &ProducerConfig{Sarama: defaultSaramaProducerConfig()}
	tlsCfg := &tls.Config{}

	if err := WithProducerTLSConfig(tlsCfg)(cfg); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !cfg.Sarama.Net.TLS.Enable {
		t.Fatal("expected TLS enabled")
	}
	if cfg.Sarama.Net.TLS.Config.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected min TLS version 1.2, got %d", cfg.Sarama.Net.TLS.Config.MinVersion)
	}
}

func TestWithProducerSASLSCRAMSHA512ConfiguresSCRAMOnly(t *testing.T) {
	cfg := &ProducerConfig{Sarama: defaultSaramaProducerConfig()}

	if err := WithProducerSASLSCRAMSHA512("user", "pass")(cfg); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !cfg.Sarama.Net.SASL.Enable {
		t.Fatal("expected SASL enabled")
	}
	if cfg.Sarama.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA512 {
		t.Fatalf("unexpected mechanism: %q", cfg.Sarama.Net.SASL.Mechanism)
	}
	if cfg.Sarama.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Fatal("expected SCRAM client generator")
	}
	if cfg.Sarama.Net.TLS.Enable {
		t.Fatal("expected TLS to remain disabled unless WithProducerTLS is used")
	}
}

func TestValidateProducerConfigSCRAMWithoutTLSIsAllowed(t *testing.T) {
	cfg := &ProducerConfig{
		Brokers: []string{"localhost:9092"},
		Retry:   RetryConfig{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		Sarama:  defaultSaramaProducerConfig(),
	}
	cfg.Sarama.Net.SASL.Enable = true
	cfg.Sarama.Net.SASL.User = "user"
	cfg.Sarama.Net.SASL.Password = "pass"
	cfg.Sarama.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	cfg.Sarama.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return nil }
	cfg.Sarama.Net.TLS.Enable = false

	err := validateProducerConfig(cfg)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestProducerSCRAMAndTLSOptionsEnableBoth(t *testing.T) {
	p, err := NewProducer(
		[]string{"localhost:9092"},
		WithProducerSASLSCRAMSHA512("user", "pass"),
		WithProducerTLS(),
	)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !p.cfg.Sarama.Net.SASL.Enable {
		t.Fatal("expected SASL enabled")
	}
	if !p.cfg.Sarama.Net.TLS.Enable {
		t.Fatal("expected TLS enabled")
	}
}

func TestProducerPublishNotReady(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = p.Publish("topic", []byte("hello"))
	if !errors.Is(err, ErrProducerNotReady) {
		t.Fatalf("expected ErrProducerNotReady, got %v", err)
	}
}

func TestProducerPublish(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock := &mockSyncProducer{}
	p.saramaProducer = mock

	err = p.Publish("topic-a", []byte("payload"))
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if mock.captured == nil {
		t.Fatal("expected captured message")
	}
	if mock.captured.Topic != "topic-a" {
		t.Fatalf("unexpected topic: %q", mock.captured.Topic)
	}
	val, encErr := mock.captured.Value.Encode()
	if encErr != nil {
		t.Fatalf("failed to encode value: %v", encErr)
	}
	if string(val) != "payload" {
		t.Fatalf("unexpected payload: %q", string(val))
	}
}

func TestProducerPublishSendError(t *testing.T) {
	p, err := NewProducer([]string{"localhost:9092"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	p.saramaProducer = &mockSyncProducer{sendErr: errors.New("send failed")}

	err = p.Publish("topic", []byte("data"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestProducerRunFailFastOnCanceledContext(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, WithProducerRetry(1, time.Nanosecond, time.Nanosecond))
	if err != nil {
		t.Fatalf("unexpected new producer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = p.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
