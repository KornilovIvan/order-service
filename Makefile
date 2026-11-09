COMPOSE ?= docker compose
PROFILE ?= tools
DSN ?= postgres://orders:orders@postgres:5432/orders?sslmode=disable
STEPS ?= 1

.PHONY: up down migrate-up migrate-down

up:
	$(COMPOSE) --profile $(PROFILE) up -d postgres

down:
	$(COMPOSE) --profile $(PROFILE) down

migrate-up:
	$(COMPOSE) --profile $(PROFILE) run --rm migrate -path=/migrations -database="$(DSN)" up

migrate-down:
	$(COMPOSE) --profile $(PROFILE) run --rm migrate -path=/migrations -database="$(DSN)" down $(STEPS)
