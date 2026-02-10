# STAGE 1: Build stage
FROM golang:1.21-alpine AS builder

# Установка зависимостей для сборки
RUN apk add --no-cache \
    gcc \
    musl-dev \
    git

# Установка рабочей директории
WORKDIR /app

# Копируем go.mod и go.sum (если они есть)
COPY go.* ./

# Скачиваем зависимости
RUN go mod download

# Копируем исходный код
COPY . .

# Сборка статически связанного бинарника
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /pipeline \
    .

# STAGE 2: Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

# Создаем рабочую директорию
WORKDIR /app

# Копируем бинарник из builder stage
COPY --from=builder /pipeline /app/pipeline

# Точка входа
ENTRYPOINT ["/app/pipeline"]