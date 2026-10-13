package api

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

// typeFromProto переводит тип из protobuf во внутренний
// Второе значение false означает, что тип не email и не push
// Enum в proto3 открытый: клиент может прислать любое число,
// поэтому default закрывает и UNSPECIFIED, и неизвестные значения
func typeFromProto(t notificationv1.NotificationType) (storage.Type, bool) {
	switch t {
	case notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL:
		return storage.TypeEmail, true
	case notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH:
		return storage.TypePush, true
	default:
		return "", false
	}
}

// typeToProto переводит внутренний тип в protobuf
func typeToProto(t storage.Type) notificationv1.NotificationType {
	switch t {
	case storage.TypeEmail:
		return notificationv1.NotificationType_NOTIFICATION_TYPE_EMAIL
	case storage.TypePush:
		return notificationv1.NotificationType_NOTIFICATION_TYPE_PUSH
	default:
		return notificationv1.NotificationType_NOTIFICATION_TYPE_UNSPECIFIED
	}
}

// statusToProto переводит внутренний статус в protobuf
func statusToProto(s storage.Status) notificationv1.NotificationStatus {
	switch s {
	case storage.StatusPending:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_PENDING
	case storage.StatusSent:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_SENT
	case storage.StatusFailed:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_FAILED
	default:
		return notificationv1.NotificationStatus_NOTIFICATION_STATUS_UNSPECIFIED
	}
}

// notificationToProto переводит запись из базы в сообщение для клиента
func notificationToProto(n storage.Notification) *notificationv1.Notification {
	return &notificationv1.Notification{
		Id:        n.ID,
		UserId:    n.UserID,
		Type:      typeToProto(n.Type),
		Status:    statusToProto(n.Status),
		Payload:   n.Payload,
		Error:     n.Error,
		CreatedAt: timestamppb.New(n.CreatedAt),
		UpdatedAt: timestamppb.New(n.UpdatedAt),
	}
}
