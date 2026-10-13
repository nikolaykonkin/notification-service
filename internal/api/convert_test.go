package api

import (
	"testing"
	"time"

	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

func TestTypeFromProto(t *testing.T) {
	tests := []struct {
		name   string
		in     notificationv1.NotificationType
		want   storage.Type
		wantOK bool
	}{
		{"email", notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL, storage.TypeEmail, true},
		{"push", notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH, storage.TypePush, true},
		{"не указан", notificationv1.NotificationType_NOTIFICATION_TYPE_UNSPECIFIED, "", false},
		{"неизвестное число", notificationv1.NotificationType(99), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := typeFromProto(tt.in)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("получено (%q, %v), ожидалось (%q, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestTypeToProto(t *testing.T) {
	if got := typeToProto(storage.TypeEmail); got != notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL {
		t.Errorf("email: получено %v", got)
	}
	if got := typeToProto(storage.TypePush); got != notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH {
		t.Errorf("push: получено %v", got)
	}
	if got := typeToProto(storage.Type("sms")); got != notificationv1.NotificationType_NOTIFICATION_TYPE_UNSPECIFIED {
		t.Errorf("неизвестный тип должен давать UNSPECIFIED, получено %v", got)
	}
}

func TestStatusToProto(t *testing.T) {
	tests := []struct {
		in   storage.Status
		want notificationv1.NotificationStatus
	}{
		{storage.StatusPending, notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING},
		{storage.StatusSent, notificationv1.NotificationStatus_NOTIFICATION_STATUS_SENT},
		{storage.StatusFailed, notificationv1.NotificationStatus_NOTIFICATION_STATUS_FAILED},
		{storage.Status("unknown"), notificationv1.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(string(tt.in), func(t *testing.T) {
			if got := statusToProto(tt.in); got != tt.want {
				t.Errorf("получено %v, ожидалось %v", got, tt.want)
			}
		})
	}
}

func TestNotificationToProto(t *testing.T) {
	created := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	updated := created.Add(time.Minute)

	n := storage.Notification{
		ID:        "11111111-1111-1111-1111-111111111111",
		UserID:    42,
		Type:      storage.TypePush,
		Status:    storage.StatusFailed,
		Payload:   `{"title": "hi"}`,
		Error:     "smtp timeout",
		CreatedAt: created,
		UpdatedAt: updated,
	}

	got := notificationToProto(n)

	if got.GetId() != n.ID || got.GetUserId() != 42 || got.GetPayload() != n.Payload || got.GetError() != n.Error {
		t.Errorf("поля скопированы неверно: %+v", got)
	}
	if got.GetType() != notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH ||
		got.GetStatus() != notificationv1.NotificationStatus_NOTIFICATION_STATUS_FAILED {
		t.Errorf("тип или статус неверны: %v, %v", got.GetType(), got.GetStatus())
	}
	if !got.GetCreatedAt().AsTime().Equal(created) || !got.GetUpdatedAt().AsTime().Equal(updated) {
		t.Errorf("время неверно: %v, %v", got.GetCreatedAt().AsTime(), got.GetUpdatedAt().AsTime())
	}
}
