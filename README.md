# go-kafka-mq

`go-kafka-mq` — лёгкая обёртка над [Sarama](https://github.com/IBM/sarama) для удобного запуска Kafka producer/consumer с retry, роутером обработчиков и опциями TLS/SASL.

## Возможности

- Sync producer с безопасным lifecycle через `Run(ctx)`
- Consumer group с роутером обработчиков по topic
- Конфигурация через options (`TLS`, `SASL`, `Kafka version`, `retry`)
- Экспоненциальные retry при создании producer/consumer

## Установка

```bash
go get github.com/Palladium-blockchain/go-kafka-mq/v2
```

## Быстрый старт

### Producer

```go
package main

import (
	"context"
	"log"
	"time"

	kafkamq "github.com/Palladium-blockchain/go-kafka-mq/v2/pkg/kafka-mq"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, err := kafkamq.NewProducer(
		[]string{"localhost:9092"},
		kafkamq.WithProducerRetry(3, time.Second, 10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := producer.Run(ctx); err != nil {
		log.Fatal(err)
	}

	if err := producer.Publish("events", []byte(`{"status":"ok"}`)); err != nil {
		log.Fatal(err)
	}
}
```

### Consumer + Router

```go
package main

import (
	"context"
	"log"
	"time"

	kafkamq "github.com/Palladium-blockchain/go-kafka-mq/v2/pkg/kafka-mq"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := kafkamq.NewRouter()
	_ = router.RegisterHandler("events", func(ctx context.Context, topic string, payload []byte) error {
		log.Printf("topic=%s payload=%s", topic, string(payload))
		return nil
	})

	consumer, err := kafkamq.NewConsumer(
		"my-consumer-group",
		[]string{"localhost:9092"},
		router,
		kafkamq.WithConsumerRetry(3, time.Second, 10*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := consumer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Опции Producer

- `WithProducerRetry(maxAttempts, initialBackoff, maxBackoff)`
- `WithProducerKafkaVersion(version)`
- `WithProducerTLS()`
- `WithProducerTLSConfig(tlsCfg)`
- `WithProducerSASLPlain(username, password)`
- `WithProducerSASLSCRAMSHA512(username, password)`

## Опции Consumer

- `WithConsumerRetry(maxAttempts, initialBackoff, maxBackoff)`
- `WithConsumerKafkaVersion(version)`
- `WithConsumerTLS()`
- `WithConsumerTLSConfig(tlsCfg)`
- `WithConsumerSASLPlain(username, password)`
- `WithConsumerSASLSCRAMSHA512(username, password)`

## Важные замечания

- `Publish` вернёт `ErrProducerNotReady`, если `Run(ctx)` ещё не был вызван.
- `With*SASLSCRAMSHA512` на producer/consumer **не включает TLS автоматически**.
- Если нужен SCRAM + TLS, используйте обе опции:
  - `WithProducerSASLSCRAMSHA512(...)` + `WithProducerTLS()`
  - `WithConsumerSASLSCRAMSHA512(...)` + `WithConsumerTLS()`

## Тесты

Локальный запуск:

```bash
make test
# или
go test ./...
```

## CI

В репозитории добавлен GitHub Actions workflow:

- `.github/workflows/test.yml` — запускает `go test ./...` на `push` и `pull_request`.
