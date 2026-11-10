COMPOSE ?= docker compose
PROFILE ?= tools
DSN ?= postgres://orders:orders@postgres:5432/orders?sslmode=disable
STEPS ?= 1

MODULE ?= order-service
PROTO_DIR ?= proto
PROTO_SRC ?= catalog.proto

.PHONY: up down migrate-up migrate-down proto

up:
	$(COMPOSE) --profile $(PROFILE) up -d postgres redis

down:
	$(COMPOSE) --profile $(PROFILE) down

migrate-up:
	$(COMPOSE) --profile $(PROFILE) run --rm migrate -path=/migrations -database="$(DSN)" up

migrate-down:
	$(COMPOSE) --profile $(PROFILE) run --rm migrate -path=/migrations -database="$(DSN)" down $(STEPS)

proto:
	protoc -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_SRC)
