FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/backuper ./cmd/backuper

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        mariadb-client \
        postgresql-client \
        restic \
        tzdata \
        openssh-client \
        sshpass \
    && if ! command -v mysqldump >/dev/null 2>&1 && command -v mariadb-dump >/dev/null 2>&1; then ln -s "$(command -v mariadb-dump)" /usr/local/bin/mysqldump; fi \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/backuper /usr/local/bin/backuper

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/backuper"]
CMD ["serve", "-config", "/app/configs/config.json", "-listen", "0.0.0.0:8080"]
