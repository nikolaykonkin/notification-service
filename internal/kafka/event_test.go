package kafka

import (
	"encoding/json"
	"testing"

	"github.com/nikolaykonkin/notification-service/internal/storage"
)

func TestTopicFor(t *testing.T) {
	tests := []struct {
		name    string
		typ     storage.Type
		want    string
		wantErr bool
	}{
		{"email", storage.TypeEmail, TopicEmail, false},
		{"push", storage.TypePush, TopicPush, false},
		{"неизвестный тип", storage.Type("sms"), "", true},
		{"пустой тип", storage.Type(""), "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TopicFor(tt.typ)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ошибка: получено %v, ожидалась ошибка: %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("топик: получено %q, ожидалось %q", got, tt.want)
			}
		})
	}
}

func TestEvent_JSONRoundTrip(t *testing.T) {
	n := storage.Notification{
		ID:      "id-1",
		UserID:  42,
		Type:    storage.TypePush,
		Payload: `{"title":"hello"}`,
	}
	want := NewEvent(n)

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Payload должен быть вложенным объектом, а не строкой с экранированием
	var generic map[string]any
	if err := json.Unmarshal(data, &generic); err != nil {
		t.Fatalf("Unmarshal в map: %v", err)
	}
	if _, ok := generic["payload"].(map[string]any); !ok {
		t.Errorf("payload должен быть JSON-объектом, получено %T", generic["payload"])
	}

	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ID != want.ID || got.UserID != want.UserID || got.Type != want.Type ||
		string(got.Payload) != string(want.Payload) {
		t.Errorf("событие изменилось:\n получено: %+v\n ожидалось: %+v", got, want)
	}
}
