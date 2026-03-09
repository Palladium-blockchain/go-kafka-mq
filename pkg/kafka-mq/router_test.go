package kafka_mq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/IBM/sarama"
)

type testSession struct {
	ctx context.Context
}

func (s *testSession) Claims() map[string][]int32 { return nil }
func (s *testSession) MemberID() string           { return "" }
func (s *testSession) GenerationID() int32        { return 0 }
func (s *testSession) MarkOffset(string, int32, int64, string) {
}
func (s *testSession) Commit() {}
func (s *testSession) ResetOffset(string, int32, int64, string) {
}
func (s *testSession) MarkMessage(*sarama.ConsumerMessage, string) {}
func (s *testSession) Context() context.Context                    { return s.ctx }

func TestRouterTopicsSorted(t *testing.T) {
	r := NewRouter()

	_ = r.RegisterHandler("z-topic", func(context.Context, string, []byte) error { return nil })
	_ = r.RegisterHandler("a-topic", func(context.Context, string, []byte) error { return nil })
	_ = r.RegisterHandler("m-topic", func(context.Context, string, []byte) error { return nil })

	got := r.Topics()
	want := []string{"a-topic", "m-topic", "z-topic"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected topics order: got %v want %v", got, want)
	}
}

func TestRouterProcessMessageTopicNotFound(t *testing.T) {
	r := NewRouter()
	s := &testSession{ctx: context.Background()}

	err := r.processMessage(s, &sarama.ConsumerMessage{Topic: "missing", Value: []byte("x")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "topic not found" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRouterProcessMessageStopsOnHandlerError(t *testing.T) {
	r := NewRouter()
	s := &testSession{ctx: context.Background()}

	calledSecond := false
	_ = r.RegisterHandler("topic", func(context.Context, string, []byte) error {
		return errors.New("handler failed")
	})
	_ = r.RegisterHandler("topic", func(context.Context, string, []byte) error {
		calledSecond = true
		return nil
	})

	err := r.processMessage(s, &sarama.ConsumerMessage{Topic: "topic", Value: []byte("x")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calledSecond {
		t.Fatal("expected second handler not to be called")
	}
}

func TestRouterProcessMessageSuccess(t *testing.T) {
	r := NewRouter()
	s := &testSession{ctx: context.Background()}

	calls := 0
	_ = r.RegisterHandler("topic", func(_ context.Context, topic string, payload []byte) error {
		calls++
		if topic != "topic" {
			t.Fatalf("unexpected topic %q", topic)
		}
		if string(payload) != "hello" {
			t.Fatalf("unexpected payload %q", string(payload))
		}
		return nil
	})
	_ = r.RegisterHandler("topic", func(context.Context, string, []byte) error {
		calls++
		return nil
	})

	err := r.processMessage(s, &sarama.ConsumerMessage{Topic: "topic", Value: []byte("hello")})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 handler calls, got %d", calls)
	}
}
