package kafka_mq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/Palladium-blockchain/go-kafka-mq/internal/scram"
)

func makeRetryConfig(maxAttempts int, initialBackoff, maxBackoff time.Duration) (RetryConfig, error) {
	if maxAttempts <= 0 {
		return RetryConfig{}, errors.New("retry max attempts must be > 0")
	}
	if initialBackoff <= 0 {
		return RetryConfig{}, errors.New("retry initial backoff must be > 0")
	}
	if maxBackoff <= 0 {
		return RetryConfig{}, errors.New("retry max backoff must be > 0")
	}

	return RetryConfig{
		MaxAttempts:    maxAttempts,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
	}, nil
}

func applyKafkaVersion(sc *sarama.Config, version string) error {
	if version == "" {
		return nil
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return fmt.Errorf("parse kafka version failed: %w", err)
	}

	sc.Version = kafkaVersion
	return nil
}

func applyTLS(sc *sarama.Config) {
	sc.Net.TLS.Enable = true
	if sc.Net.TLS.Config == nil {
		sc.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	} else if sc.Net.TLS.Config.MinVersion == 0 {
		sc.Net.TLS.Config.MinVersion = tls.VersionTLS12
	}
}

func applyTLSConfig(sc *sarama.Config, tlsCfg *tls.Config) error {
	if tlsCfg == nil {
		return errors.New("tls config is nil")
	}
	if tlsCfg.MinVersion == 0 {
		tlsCfg.MinVersion = tls.VersionTLS12
	}

	sc.Net.TLS.Enable = true
	sc.Net.TLS.Config = tlsCfg
	return nil
}

func applySASLPlain(sc *sarama.Config, username, password string) error {
	if username == "" {
		return errors.New("plain username is empty")
	}
	if password == "" {
		return errors.New("plain password is empty")
	}

	sc.Net.SASL.Enable = true
	sc.Net.SASL.User = username
	sc.Net.SASL.Password = password
	sc.Net.SASL.Handshake = true
	sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	return nil
}

func applySASLSCRAMSHA512(sc *sarama.Config, username, password string) error {
	if username == "" {
		return errors.New("scram username is empty")
	}
	if password == "" {
		return errors.New("scram password is empty")
	}

	sc.Net.SASL.Enable = true
	sc.Net.SASL.User = username
	sc.Net.SASL.Password = password
	sc.Net.SASL.Handshake = true
	sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
	}
	return nil
}

func validateRetryConfig(cfg RetryConfig) error {
	if cfg.MaxAttempts <= 0 {
		return errors.New("retry max attempts must be > 0")
	}
	if cfg.InitialBackoff <= 0 {
		return errors.New("retry initial backoff must be > 0")
	}
	if cfg.MaxBackoff <= 0 {
		return errors.New("retry max backoff must be > 0")
	}
	return nil
}

func validateSASLConfig(sc *sarama.Config) error {
	if !sc.Net.SASL.Enable {
		return nil
	}

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

	return nil
}
