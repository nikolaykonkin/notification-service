package api

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

func TestValidateSend(t *testing.T) {
	email := notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL
	push := notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH

	tests := []struct {
		name     string
		req      *notificationv1.SendNotificationRequest
		wantType storage.Type
		wantErr  bool
	}{
		{"email", &notificationv1.SendNotificationRequest{UserId: 1, Type: email, Payload: `{"a":1}`}, storage.TypeEmail, false},
		{"push", &notificationv1.SendNotificationRequest{UserId: 1, Type: push, Payload: `{}`}, storage.TypePush, false},
		{"нулевой user_id", &notificationv1.SendNotificationRequest{UserId: 0, Type: email, Payload: `{}`}, "", true},
		{"отрицательный user_id", &notificationv1.SendNotificationRequest{UserId: -5, Type: email, Payload: `{}`}, "", true},
		{"тип не указан", &notificationv1.SendNotificationRequest{UserId: 1, Payload: `{}`}, "", true},
		{"пустой payload", &notificationv1.SendNotificationRequest{UserId: 1, Type: email}, "", true},
		{"payload не JSON", &notificationv1.SendNotificationRequest{UserId: 1, Type: email, Payload: "not json"}, "", true},
		{
			"payload слишком большой",
			&notificationv1.SendNotificationRequest{UserId: 1, Type: email, Payload: `"` + strings.Repeat("a", maxPayloadBytes) + `"`},
			"", true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateSend(tt.req)
			if tt.wantErr {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("ожидался код InvalidArgument, получено %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("неожиданная ошибка: %v", err)
			}
			if got != tt.wantType {
				t.Errorf("тип: получено %q, ожидалось %q", got, tt.wantType)
			}
		})
	}
}

func TestValidUUID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"строчные", "11111111-1111-1111-1111-111111111111", true},
		{"смешанный регистр", "AbCdEf01-2345-6789-abcd-ef0123456789", true},
		{"пустая строка", "", false},
		{"не UUID", "not-a-uuid", false},
		{"нужной длины, но без дефисов", "111111111111111111111111111111111111", false},
		{"не шестнадцатеричная цифра", "g1111111-1111-1111-1111-111111111111", false},
		{"слишком длинная", "11111111-1111-1111-1111-1111111111111", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validUUID(tt.in); got != tt.want {
				t.Errorf("validUUID(%q) = %v, ожидалось %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidateID(t *testing.T) {
	if err := validateID("11111111-1111-1111-1111-111111111111"); err != nil {
		t.Errorf("корректный id отклонен: %v", err)
	}
	if err := validateID("../../etc/passwd"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("ожидался InvalidArgument, получено %v", err)
	}
}
