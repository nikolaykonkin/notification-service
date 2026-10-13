package api

import (
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nikolaykonkin/notification-service/internal/storage"
	notificationv1 "github.com/nikolaykonkin/notification-service/proto/notification/v1"
)

// maxPayloadBytes ограничивает размер payload: он попадает в JSONB и в сообщение Kafka
// (по умолчанию брокер принимает сообщения до 1 МБ)
const maxPayloadBytes = 64 << 10

// validateSend проверяет запрос на отправку и возвращает внутренний тип уведомления
// Ошибки сразу возвращаются в виде gRPC-статусов
func validateSend(req *notificationv1.SendNotificationRequest) (storage.Type, error) {
	if req.GetUserId() <= 0 {
		return "", status.Error(codes.InvalidArgument, "user_id должен быть больше нуля")
	}

	typ, ok := typeFromProto(req.GetType())
	if !ok {
		return "", status.Error(codes.InvalidArgument, "type должен быть EMAIL или PUSH")
	}

	payload := req.GetPayload()
	switch {
	case payload == "":
		return "", status.Error(codes.InvalidArgument, "payload не должен быть пустым")
	case len(payload) > maxPayloadBytes:
		return "", status.Errorf(codes.InvalidArgument, "payload не должен быть больше %d байт", maxPayloadBytes)
	case !json.Valid([]byte(payload)):
		return "", status.Error(codes.InvalidArgument, "payload должен быть корректным JSON")
	}

	return typ, nil
}

// validateID проверяет, что id похож на UUID
// Так мусор не доходит ни до кеша, ни до базы
func validateID(id string) error {
	if !validUUID(id) {
		return status.Error(codes.InvalidArgument, "id должен быть UUID вида xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	}
	return nil
}

// validUUID проверяет форму 8-4-4-4-12 из шестнадцатеричных цифр
func validUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
			if !isHex {
				return false
			}
		}
	}
	return true
}
