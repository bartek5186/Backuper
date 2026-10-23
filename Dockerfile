
FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /out/backuper ./cmd/backuper

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        mariadb-client \
        restic \
        tzdata \
        openssh-client \
        sshpass; \
    install -d /usr/share/postgresql-common/pgdg; \
    curl -fsSL \
      https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc; \
    printf '%s\n' \
      'Types: deb' \
      'URIs: https://apt.postgresql.org/pub/repos/apt' \
      'Suites: bookworm-pgdg' \
      'Components: main' \
      'Signed-By: /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc' \
      > /etc/apt/sources.list.d/pgdg.sources; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        postgresql-client-18; \
    if ! command -v mysqldump >/dev/null 2>&1; then \
        ln -s "$(command -v mariadb-dump)" \
            /usr/local/bin/mysqldump; \
    fi; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /out/backuper /usr/local/bin/backuper

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/backuper"]

CMD ["serve", "-config", "/app/configs/config.json", "-listen", "0.0.0.0:8080"]
