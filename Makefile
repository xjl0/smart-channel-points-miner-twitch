# Точки входа для разработчика. Список: `make help`.

.PHONY: help gen proto sqlc web run test lint docker

help:            ## Показать эту справку
	@grep -E '^[a-z]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

gen: proto sqlc  ## Сгенерировать весь код (protobuf + sqlc)

proto:           ## Сгенерировать Go- и TS-код из proto/
	buf generate

sqlc:            ## Сгенерировать типизированные SQL-запросы
	sqlc generate

web:             ## Собрать админ-UI в web/dist
	cd web && npm run build

run:             ## Запустить бэкенд локально
	go run ./cmd/miner

test:            ## Запустить Go-тесты
	go test ./...

lint:            ## Проверить линтером proto и Go
	buf lint
	go vet ./...

docker:          ## Собрать оба образа (Go-бэкенд и Caddy с админкой)
	docker compose build
