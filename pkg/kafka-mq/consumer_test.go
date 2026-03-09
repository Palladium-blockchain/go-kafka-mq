package kafka_mq

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestNewConsumerValidation(t *testing.T) {
	r := NewRouter()

	if _, err := NewConsumer("", []string{"localhost:9092"}, r); err == nil {
		t.Fatal("expected consumer group validation error")
	}
	if _, err := NewConsumer("group", nil, r); err == nil {
		t.Fatal("expected brokers validation error")
	}
	if _, err := NewConsumer("group", []string{"localhost:9092"}, nil); err == nil {
		t.Fatal("expected router validation error")
	}
}

func TestNewConsumerValidDefaultConfig(t *testing.T) {
	c, err := NewConsumer("group", []string{"localhost:9092"}, NewRouter())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if c == nil || c.cfg == nil || c.cfg.Sarama == nil {
		t.Fatal("expected non-nil consumer and config")
	}
}

func TestWithConsumerRetryValidation(t *testing.T) {
	cfg := &ConsumerConfig{Sarama: defaultSaramaConsumerConfig()}

	if err := WithConsumerRetry(0, time.Second, time.Second)(cfg); err == nil {
		t.Fatal("expected maxAttempts validation error")
	}
	if err := WithConsumerRetry(1, 0, time.Second)(cfg); err == nil {
		t.Fatal("expected initialBackoff validation error")
	}
	if err := WithConsumerRetry(1, time.Second, 0)(cfg); err == nil {
		t.Fatal("expected maxBackoff validation error")
	}
}

func TestWithConsumerDialTimeout(t *testing.T) {
	cfg := &ConsumerConfig{Sarama: defaultSaramaConsumerConfig()}

	if err := WithConsumerDialTimeout(3 * time.Second)(cfg); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if cfg.Sarama.Net.DialTimeout != 3*time.Second {
		t.Fatalf("expected dial timeout 3s, got %v", cfg.Sarama.Net.DialTimeout)
	}
}

func TestWithConsumerDialTimeoutValidation(t *testing.T) {
	cfg := &ConsumerConfig{Sarama: defaultSaramaConsumerConfig()}

	if err := WithConsumerDialTimeout(0)(cfg); err == nil {
		t.Fatal("expected dial timeout validation error")
	}
}

func TestWithConsumerTLSConfigSetsMinVersion(t *testing.T) {
	cfg := &ConsumerConfig{Sarama: defaultSaramaConsumerConfig()}
	tlsCfg := &tls.Config{}

	if err := WithConsumerTLSConfig(tlsCfg)(cfg); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !cfg.Sarama.Net.TLS.Enable {
		t.Fatal("expected TLS enabled")
	}
	if cfg.Sarama.Net.TLS.Config.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected min TLS version 1.2, got %d", cfg.Sarama.Net.TLS.Config.MinVersion)
	}
}

func TestWithConsumerSASLSCRAMSHA512ConfiguresSCRAMOnly(t *testing.T) {
	cfg := &ConsumerConfig{Sarama: defaultSaramaConsumerConfig()}

	if err := WithConsumerSASLSCRAMSHA512("user", "pass")(cfg); err != nil {
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
		t.Fatal("expected TLS to remain disabled unless WithConsumerTLS is used")
	}
}

func TestValidateConsumerConfigSCRAMWithoutTLSIsAllowed(t *testing.T) {
	cfg := &ConsumerConfig{
		ConsumerGroup: "group",
		Brokers:       []string{"localhost:9092"},
		Retry:         RetryConfig{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		Sarama:        defaultSaramaConsumerConfig(),
	}
	cfg.Sarama.Net.SASL.Enable = true
	cfg.Sarama.Net.SASL.User = "user"
	cfg.Sarama.Net.SASL.Password = "pass"
	cfg.Sarama.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	cfg.Sarama.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return nil }
	cfg.Sarama.Net.TLS.Enable = false

	err := validateConsumerConfig(cfg)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestConsumerSCRAMAndTLSOptionsEnableBoth(t *testing.T) {
	c, err := NewConsumer(
		"group",
		[]string{"localhost:9092"},
		NewRouter(),
		WithConsumerSASLSCRAMSHA512("user", "pass"),
		WithConsumerTLS(),
	)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !c.cfg.Sarama.Net.SASL.Enable {
		t.Fatal("expected SASL enabled")
	}
	if !c.cfg.Sarama.Net.TLS.Enable {
		t.Fatal("expected TLS enabled")
	}
}

func TestValidateConsumerConfigUnsupportedMechanism(t *testing.T) {
	cfg := &ConsumerConfig{
		ConsumerGroup: "group",
		Brokers:       []string{"localhost:9092"},
		Retry:         RetryConfig{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
		Sarama:        defaultSaramaConsumerConfig(),
	}
	cfg.Sarama.Net.SASL.Enable = true
	cfg.Sarama.Net.SASL.User = "user"
	cfg.Sarama.Net.SASL.Password = "pass"
	cfg.Sarama.Net.SASL.Mechanism = sarama.SASLMechanism("GSSAPI")

	err := validateConsumerConfig(cfg)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestConsumerRunFailFastOnCanceledContext(t *testing.T) {
	r := NewRouter()
	_ = r.RegisterHandler("events", func(context.Context, string, []byte) error { return nil })

	c, err := NewConsumer("group", []string{"localhost:9092"}, r)
	if err != nil {
		t.Fatalf("unexpected new consumer error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = c.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
