COMPOSE ?= docker compose
PROFILE ?= tools
DSN ?= postgres://orders:orders@postgres:5432/orders?sslmode=disable
STEPS ?= 1
# Собираем сервисы по одному: параллельная сборка четырёх образов
# на 8-ядерной VM съедает всю память и подвешивает ноутбук.
COMPOSE_PARALLEL_LIMIT ?= 1
export COMPOSE_PARALLEL_LIMIT

MODULE ?= order-service
PROTOC ?= protoc
PROTO_DIR ?= proto
JOBS_PROTO_DIR ?= api/proto
WELL_KNOWN_INCLUDE ?= $(shell dirname $(shell command -v $(PROTOC)))/../include
PROTO_SRCS ?= catalog.proto jobs/v1/jobs.proto

.PHONY: up down migrate-up migrate-down proto lint

up:
	$(COMPOSE) up -d --build
	$(COMPOSE) --profile $(PROFILE) run --rm migrate -path=/migrations -database="$(DSN)" up

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

# Тот же образ, что в CI. Первый запуск скачивает тулчейн и модули,
# поэтому кеш вынесен в тома и переживает запуски.
lint:
	docker run --rm -v "$(CURDIR)":/app -w /app \
		-v golangci-mod:/go/pkg/mod \
		-v golangci-build:/root/.cache/go-build \
		golangci/golangci-lint:v1.64.8 golangci-lint run --timeout 10m ./...
