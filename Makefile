COMPOSE ?= docker compose
PROFILE ?= tools
DSN ?= postgres://orders:orders@postgres:5432/orders?sslmode=disable
STEPS ?= 1

MODULE ?= order-service
PROTOC ?= protoc
PROTO_DIR ?= proto
JOBS_PROTO_DIR ?= api/proto
WELL_KNOWN_INCLUDE ?= $(shell dirname $(shell command -v $(PROTOC)))/../include
PROTO_SRCS ?= catalog.proto jobs/v1/jobs.proto

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
	$(PROTOC) -I $(PROTO_DIR) -I $(JOBS_PROTO_DIR) -I $(WELL_KNOWN_INCLUDE) \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_SRCS)
