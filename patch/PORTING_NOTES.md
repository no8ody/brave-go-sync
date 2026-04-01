# Porting notes

## Scope

This patch set does three things:

1. Replaces the DynamoDB-backed datastore with PostgreSQL.
2. Makes Redis optional by providing an in-memory cache fallback.
3. Publishes a Docker image to Docker Hub only after CI succeeds.

## Layout

The refactor separates shared datastore models from backend-specific implementation.
The old mixed DynamoDB files were removed and replaced with smaller files that are easier to compare during upstream updates.

Current PostgreSQL-related files:

- `datastore/postgres.go`
- `datastore/entity.go`
- `datastore/client_item_counts.go`

Cache-related changes:

- `cache/memory.go`
- `cache/redis.go`

Workflow-related changes:

- `.github/workflows/ci.yml`
- `.github/workflows/docker-publish.yml`

Server wiring:

- `server/server.go`
- `command/command.go`
- `command/server_defined_unique_entity.go`

## Merge strategy for future upstream updates

1. Start from a fresh upstream tree.
2. Apply the files from this patch set.
3. Remove the paths listed in `DELETE_FILES.txt`.
4. Review upstream changes in the files listed above before merging.
5. Rebuild the unified diff if the patch set changes.

## Module maintenance

Keep `go.mod` and `go.sum` in sync after dependency changes. The PostgreSQL backend adds `pgx` transitives that must remain in the tidy module graph for Go 1.24 CI runs.

## Areas that may need manual review

- Any upstream change touching datastore interfaces or entity validation.
- Any upstream change touching cache initialization or Redis handling.
- Any upstream change touching GitHub Actions workflow names or trigger logic.
- Any upstream change touching Dockerfile, runtime environment variables, or compose files.
