# Файл схемы gRPC-сервиса
PROTO_FILE := proto/notification/v1/notification.proto

# Генерация Go-кода из .proto: структуры (*.pb.go) и gRPC-код (*_grpc.pb.go)
# paths=source_relative кладет результат рядом с .proto-файлом
.PHONY: proto
proto:
	protoc \
		--proto_path=. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)