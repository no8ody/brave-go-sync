# Brave Sync Server v2

A sync server implemented in Go to communicate with Brave sync clients using
[components/sync/protocol/sync.proto](https://cs.chromium.org/chromium/src/components/sync/protocol/sync.proto).
Current Chromium version for sync protocol buffer files used in this repo is Chromium 116.0.5845.183.

This server supports the following endpoint:
- `POST /v2/command/` handles Commit and GetUpdates requests from sync clients and returns protobuf responses. Detailed request and response shapes are defined in `schema/protobuf/sync_pb/sync.proto`.

## What changed in this fork

This fork is set up for self-hosting with:
- PostgreSQL as the datastore
- an in-memory cache by default, so Redis is optional
- a dedicated GitHub Actions workflow that builds the Docker image and pushes it to Docker Hub only after the `ci` workflow succeeds on `master`

The Docker workflow publishes:
- `no8odycares/brave-go-sync:latest`
- `no8odycares/brave-go-sync:sha-<full commit sha>`

## Runtime configuration

### Required
- `DATABASE_URL` – PostgreSQL connection string, for example `postgres://user:password@host:5432/brave_sync?sslmode=disable`

### Optional
- `CACHE_BACKEND` – set to `memory` to force in-memory cache; this is the default when `REDIS_URL` is not set
- `REDIS_URL` – Redis endpoint if you prefer Redis over the in-memory cache
- `POSTGRES_MAX_CONNS`
- `POSTGRES_MIN_CONNS`
- `PPROF_ENABLED`
- `SENTRY_DSN`
- `ENV`
- `DEBUG`

## GitHub Actions / Docker Hub setup

Add these repository secrets in GitHub:
- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

The workflow file is `.github/workflows/docker-publish.yml`.
After a successful `ci` run on `master`, GitHub Actions will:
1. build the image for the same commit that passed CI
2. tag it with `latest`
3. tag it with the commit SHA
4. push both tags to Docker Hub

## Local Docker usage

Only the `web` service is defined in `docker-compose.yml`.
You provide the PostgreSQL connection externally via `DATABASE_URL`.

Example:

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/brave_sync?sslmode=disable'
make docker
make docker-up
```

To connect to your local server from a Brave browser client use:

```bash
--sync-url="http://localhost:8295/v2"
```

## Developer setup

1. Install Go
2. Install GolangCI-Lint
3. Install gowrap
4. Clone this repo
5. Install protobuf protocol compiler if you need to compile protobuf files
6. Build via `make`

## Notes on tests

The legacy DynamoDB-local integration tests and helper files were removed as part of the PostgreSQL refactor. CI still uses a compile-style Go test pass (`go test -run '^$' ./...`) to verify the codebase without requiring an external database service.

## Updating protocol definitions

1. Copy the `.proto` files from `components/sync/protocol` in `chromium` to `schema/protobuf/sync_pb` in `go-sync`.
2. Copy the `.proto` files from `components/sync/protocol` in `brave-core` to `schema/protobuf/sync_pb` in `go-sync`.
3. Run `make repath-proto` to set correct import paths in `.proto` files.
4. Run `make proto-go-module` to add the `go_module` option to `.proto` files.
5. Run `make protobuf` to generate the Go code from `.proto` definitions.

## Prometheus instrumentation

The instrumented datastore and cache interfaces are generated. Re-generate them with:

```bash
make instrumented
```

Changes to `datastore/datastore.go` or `cache/cache.go` should be followed by the above command.
