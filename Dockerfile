# syntax=docker/dockerfile:1

# Один Dockerfile на все четыре бинаря (api, worker, catalogfake, erpfake).
# Какой из них попадёт в образ — решает ARG BINARY, поэтому в compose один
# и тот же файл используется с разными args, а не четыре почти одинаковых.

# alpine вместо полного debian-образа: ~250 МБ вместо ~800 МБ скачивания
# и распаковки. CGO выключен, бинарь статический — debian не нужен.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Сначала только манифесты модулей: слой с зависимостями кешируется
# независимо от изменений в коде (и переиспользуется между BINARY-сборками).
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Все четыре бинаря собираются ОДНИМ вызовом: зависимости компилируются
# один раз, а не по разу на сервис. -p 2 ограничивает параллелизм
# компилятора: по умолчанию Go берёт GOMAXPROCS, и на 8-ядерной VM
# четыре параллельные сборки съедают всю память.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -p 2 \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/ ./cmd/...

# distroless: нет shell и пакетного менеджера, только бинарь и CA-сертификаты.
# Запуск от nonroot — процесс не может писать в ФС образа.
FROM gcr.io/distroless/static-debian12:nonroot

ARG BINARY=api
COPY --from=build /out/${BINARY} /service

USER nonroot:nonroot
ENTRYPOINT ["/service"]
