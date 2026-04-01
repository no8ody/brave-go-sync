package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const uniqueViolationCode = "23505"

// Postgres is a Datastore implementation backed by PostgreSQL.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres creates a PostgreSQL datastore and bootstraps the required
// schema.
func NewPostgres() (*Postgres, error) {
	databaseURL, err := getDatabaseURL()
	if err != nil {
		return nil, err
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing postgres configuration: %w", err)
	}

	if maxConns, err := strconv.ParseInt(os.Getenv("POSTGRES_MAX_CONNS"), 10, 32); err == nil && maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	if minConns, err := strconv.ParseInt(os.Getenv("POSTGRES_MIN_CONNS"), 10, 32); err == nil && minConns >= 0 {
		cfg.MinConns = int32(minConns)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating postgres pool: %w", err)
	}

	db := &Postgres{pool: pool}
	if err := db.initSchema(context.Background()); err != nil {
		pool.Close()
		return nil, err
	}

	return db, nil
}

func getDatabaseURL() (string, error) {
	if databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL")); databaseURL != "" {
		return databaseURL, nil
	}

	host := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
	if host == "" {
		return "", errors.New("DATABASE_URL or POSTGRES_HOST must be set")
	}

	port := strings.TrimSpace(os.Getenv("POSTGRES_PORT"))
	if port == "" {
		port = "5432"
	}

	user := strings.TrimSpace(os.Getenv("POSTGRES_USER"))
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	database := strings.TrimSpace(os.Getenv("POSTGRES_DB"))
	if database == "" {
		database = "brave_sync"
	}

	sslMode := strings.TrimSpace(os.Getenv("POSTGRES_SSLMODE"))
	if sslMode == "" {
		sslMode = "disable"
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, port),
		Path:   database,
	}

	q := url.Values{}
	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (pg *Postgres) initSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS sync_entities (
	client_id TEXT NOT NULL,
	id TEXT NOT NULL,
	version BIGINT NOT NULL,
	data_type INTEGER NOT NULL,
	mtime BIGINT NOT NULL,
	folder BOOLEAN NOT NULL DEFAULT FALSE,
	client_defined_unique_tag TEXT,
	server_defined_unique_tag TEXT,
	deleted BOOLEAN NOT NULL DEFAULT FALSE,
	expiration_time BIGINT,
	entity JSONB NOT NULL,
	PRIMARY KEY (client_id, id)
);

CREATE INDEX IF NOT EXISTS idx_sync_entities_updates
	ON sync_entities (client_id, data_type, mtime, id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_entities_client_unique_tag
	ON sync_entities (client_id, client_defined_unique_tag)
	WHERE client_defined_unique_tag IS NOT NULL AND deleted = FALSE AND data_type <> 963985;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_entities_server_unique_tag
	ON sync_entities (client_id, server_defined_unique_tag)
	WHERE server_defined_unique_tag IS NOT NULL;

CREATE TABLE IF NOT EXISTS client_item_counts (
	client_id TEXT PRIMARY KEY,
	item_count INTEGER NOT NULL DEFAULT 0,
	history_item_count_period1 INTEGER NOT NULL DEFAULT 0,
	history_item_count_period2 INTEGER NOT NULL DEFAULT 0,
	history_item_count_period3 INTEGER NOT NULL DEFAULT 0,
	history_item_count_period4 INTEGER NOT NULL DEFAULT 0,
	last_period_change_time BIGINT NOT NULL DEFAULT 0,
	version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS disabled_sync_chains (
	client_id TEXT PRIMARY KEY,
	reason TEXT NOT NULL,
	mtime BIGINT NOT NULL,
	ctime BIGINT NOT NULL
);
`

	_, err := pg.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("error initializing postgres schema: %w", err)
	}
	return nil
}

func marshalEntity(entity *SyncEntity) ([]byte, error) {
	payload, err := json.Marshal(entity)
	if err != nil {
		return nil, fmt.Errorf("error marshalling sync entity: %w", err)
	}
	return payload, nil
}

func unmarshalEntity(payload []byte) (SyncEntity, error) {
	var entity SyncEntity
	if err := json.Unmarshal(payload, &entity); err != nil {
		return SyncEntity{}, fmt.Errorf("error unmarshalling sync entity: %w", err)
	}
	return entity, nil
}

func boolValue(v *bool) bool {
	return v != nil && *v
}

func intValue(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

func int64Value(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode
}
func nullableString(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func (pg *Postgres) insertEntity(ctx context.Context, q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, entity *SyncEntity) error {
	payload, err := marshalEntity(entity)
	if err != nil {
		return err
	}

	_, err = q.Exec(ctx, `
INSERT INTO sync_entities (
	client_id,
	id,
	version,
	data_type,
	mtime,
	folder,
	client_defined_unique_tag,
	server_defined_unique_tag,
	deleted,
	expiration_time,
	entity
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
`,
		entity.ClientID,
		entity.ID,
		int64Value(entity.Version),
		intValue(entity.DataType),
		int64Value(entity.Mtime),
		boolValue(entity.Folder),
		nullableString(entity.ClientDefinedUniqueTag),
		nullableString(entity.ServerDefinedUniqueTag),
		boolValue(entity.Deleted),
		nullableInt64(entity.ExpirationTime),
		payload,
	)
	if err != nil {
		return fmt.Errorf("error inserting sync entity: %w", err)
	}
	return nil
}

// InsertSyncEntity inserts a new sync entity into PostgreSQL.
func (pg *Postgres) InsertSyncEntity(ctx context.Context, entity *SyncEntity) (bool, error) {
	if err := pg.insertEntity(ctx, pg.pool, entity); err != nil {
		if isUniqueViolation(err) {
			return true, err
		}
		return false, err
	}
	return false, nil
}

// HasServerDefinedUniqueTag checks if the server-defined unique tag already
// exists for the client.
func (pg *Postgres) HasServerDefinedUniqueTag(ctx context.Context, clientID string, tag string) (bool, error) {
	var exists bool
	err := pg.pool.QueryRow(ctx, `
SELECT EXISTS(
	SELECT 1
	FROM sync_entities
	WHERE client_id = $1 AND server_defined_unique_tag = $2
)
`, clientID, tag).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking server-defined unique tag: %w", err)
	}
	return exists, nil
}

func (pg *Postgres) HasItem(ctx context.Context, clientID string, ID string) (bool, error) {
	var exists bool
	err := pg.pool.QueryRow(ctx, `
SELECT EXISTS(
	SELECT 1
	FROM sync_entities
	WHERE client_id = $1 AND id = $2
)
`, clientID, ID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking whether sync item exists: %w", err)
	}
	return exists, nil
}

// InsertSyncEntitiesWithServerTags inserts multiple sync entities that carry
// server-defined unique tags in a single transaction.
func (pg *Postgres) InsertSyncEntitiesWithServerTags(ctx context.Context, entities []*SyncEntity) error {
	tx, err := pg.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("error opening transaction for server-defined entities: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, entity := range entities {
		if err := pg.insertEntity(ctx, tx, entity); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing server-defined entity transaction: %w", err)
	}
	return nil
}

// DisableSyncChain marks a sync chain as disabled.
func (pg *Postgres) DisableSyncChain(ctx context.Context, clientID string) error {
	now := time.Now().UnixMilli()
	_, err := pg.pool.Exec(ctx, `
INSERT INTO disabled_sync_chains (client_id, reason, mtime, ctime)
VALUES ($1, $2, $3, $4)
ON CONFLICT (client_id) DO UPDATE SET
	reason = EXCLUDED.reason,
	mtime = EXCLUDED.mtime,
	ctime = EXCLUDED.ctime
`, clientID, reasonDeleted, now, now)
	if err != nil {
		return fmt.Errorf("error disabling sync chain: %w", err)
	}
	return nil
}

// ClearServerData deletes all sync entities and count rows for a client while
// keeping the disabled-chain marker intact.
func (pg *Postgres) ClearServerData(ctx context.Context, clientID string) ([]SyncEntity, error) {
	tx, err := pg.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("error opening clear-server-data transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
SELECT entity
FROM sync_entities
WHERE client_id = $1
ORDER BY mtime, id
`, clientID)
	if err != nil {
		return nil, fmt.Errorf("error reading sync entities before clearing: %w", err)
	}

	entities := make([]SyncEntity, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			rows.Close()
			return nil, fmt.Errorf("error scanning sync entity before clearing: %w", err)
		}
		entity, err := unmarshalEntity(payload)
		if err != nil {
			rows.Close()
			return nil, err
		}
		entities = append(entities, entity)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("error iterating sync entities before clearing: %w", err)
	}
	rows.Close()

	if _, err := tx.Exec(ctx, `DELETE FROM sync_entities WHERE client_id = $1`, clientID); err != nil {
		return nil, fmt.Errorf("error deleting sync entities for client %s: %w", clientID, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM client_item_counts WHERE client_id = $1`, clientID); err != nil {
		return nil, fmt.Errorf("error deleting client item counts for client %s: %w", clientID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing clear-server-data transaction: %w", err)
	}
	return entities, nil
}

// IsSyncChainDisabled checks whether the chain was disabled.
func (pg *Postgres) IsSyncChainDisabled(ctx context.Context, clientID string) (bool, error) {
	var exists bool
	err := pg.pool.QueryRow(ctx, `
SELECT EXISTS(
	SELECT 1
	FROM disabled_sync_chains
	WHERE client_id = $1
)
`, clientID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error checking disabled sync chain: %w", err)
	}
	return exists, nil
}

func mergeSyncEntity(existing SyncEntity, update *SyncEntity) SyncEntity {
	merged := existing
	merged.Version = update.Version
	merged.Mtime = update.Mtime
	merged.Specifics = update.Specifics
	merged.DataTypeMtime = update.DataTypeMtime
	merged.ExpirationTime = update.ExpirationTime

	if update.UniquePosition != nil {
		merged.UniquePosition = update.UniquePosition
	}
	if update.ParentID != nil {
		merged.ParentID = update.ParentID
	}
	if update.Name != nil {
		merged.Name = update.Name
	}
	if update.NonUniqueName != nil {
		merged.NonUniqueName = update.NonUniqueName
	}
	if update.Deleted != nil {
		merged.Deleted = update.Deleted
	}
	if update.Folder != nil {
		merged.Folder = update.Folder
	}

	return merged
}

// UpdateSyncEntity updates an existing sync entity using optimistic locking.
func (pg *Postgres) UpdateSyncEntity(ctx context.Context, entity *SyncEntity, oldVersion int64) (bool, bool, error) {
	tx, err := pg.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, false, fmt.Errorf("error opening update transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var dbVersion int64
	var payload []byte
	err = tx.QueryRow(ctx, `
SELECT version, entity
FROM sync_entities
WHERE client_id = $1 AND id = $2
FOR UPDATE
`, entity.ClientID, entity.ID).Scan(&dbVersion, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("error reading sync entity to update: %w", err)
	}

	existing, err := unmarshalEntity(payload)
	if err != nil {
		return false, false, err
	}

	if intValue(entity.DataType) != HistoryTypeID && dbVersion != oldVersion {
		return true, false, nil
	}

	merged := mergeSyncEntity(existing, entity)
	mergedPayload, err := marshalEntity(&merged)
	if err != nil {
		return false, false, err
	}

	_, err = tx.Exec(ctx, `
UPDATE sync_entities
SET version = $1,
	mtime = $2,
	folder = $3,
	deleted = $4,
	expiration_time = $5,
	entity = $6
WHERE client_id = $7 AND id = $8
`,
		int64Value(merged.Version),
		int64Value(merged.Mtime),
		boolValue(merged.Folder),
		boolValue(merged.Deleted),
		nullableInt64(merged.ExpirationTime),
		mergedPayload,
		merged.ClientID,
		merged.ID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return true, false, nil
		}
		return false, false, fmt.Errorf("error updating sync entity: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, false, fmt.Errorf("error committing sync entity update: %w", err)
	}

	deleted := entity.Deleted != nil && !boolValue(existing.Deleted) && boolValue(merged.Deleted)
	return false, deleted, nil
}

// GetUpdatesForType returns entities for a type that changed after the client
// token.
func (pg *Postgres) GetUpdatesForType(ctx context.Context, dataType int, clientToken int64, fetchFolders bool, clientID string, maxSize int64) (bool, []SyncEntity, error) {
	if maxSize <= 0 {
		maxSize = 1
	}

	nowUnix := time.Now().Unix()
	limit := maxSize + 1
	query := `
SELECT entity
FROM sync_entities
WHERE client_id = $1
  AND data_type = $2
  AND mtime > $3
  AND (expiration_time IS NULL OR expiration_time > $4)
`
	args := []any{clientID, dataType, clientToken, nowUnix}
	if !fetchFolders {
		query += ` AND folder = FALSE`
	}
	query += ` ORDER BY mtime ASC, id ASC LIMIT $5`
	args = append(args, limit)

	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return false, nil, fmt.Errorf("error querying updates for type: %w", err)
	}
	defer rows.Close()

	entities := make([]SyncEntity, 0, int(maxSize))
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return false, nil, fmt.Errorf("error scanning update entity: %w", err)
		}
		entity, err := unmarshalEntity(payload)
		if err != nil {
			return false, nil, err
		}
		entities = append(entities, entity)
	}
	if err := rows.Err(); err != nil {
		return false, nil, fmt.Errorf("error iterating update entities: %w", err)
	}

	hasChangesRemaining := int64(len(entities)) > maxSize
	if hasChangesRemaining {
		entities = entities[:int(maxSize)]
	}
	return hasChangesRemaining, entities, nil
}

func (pg *Postgres) initRealCountsAndUpdateHistoryCounts(ctx context.Context, counts *ClientItemCounts) error {
	now := time.Now().Unix()
	if counts.Version < CurrentCountVersion {
		if counts.ItemCount > 0 || counts.SumHistoryCounts() > 0 {
			var historyCount int
			if err := pg.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM sync_entities
WHERE client_id = $1
  AND data_type IN ($2, $3)
  AND deleted = FALSE
  AND (expiration_time IS NULL OR expiration_time > $4)
`, counts.ClientID, HistoryTypeID, HistoryDeleteDirectiveTypeID, now).Scan(&historyCount); err != nil {
				return fmt.Errorf("error querying history item count: %w", err)
			}
			counts.HistoryItemCountPeriod1 = 0
			counts.HistoryItemCountPeriod2 = 0
			counts.HistoryItemCountPeriod3 = 0
			counts.HistoryItemCountPeriod4 = historyCount

			var normalCount int
			if err := pg.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM sync_entities
WHERE client_id = $1
  AND data_type NOT IN ($2, $3)
  AND deleted = FALSE
`, counts.ClientID, HistoryTypeID, HistoryDeleteDirectiveTypeID).Scan(&normalCount); err != nil {
				return fmt.Errorf("error querying normal item count: %w", err)
			}
			counts.ItemCount = normalCount
		}
		counts.LastPeriodChangeTime = now
		counts.Version = CurrentCountVersion
	} else {
		timeSinceLastChange := now - counts.LastPeriodChangeTime
		if timeSinceLastChange >= periodDurationSecs {
			changeCount := int(timeSinceLastChange / periodDurationSecs)
			for range changeCount {
				counts.HistoryItemCountPeriod1 = counts.HistoryItemCountPeriod2
				counts.HistoryItemCountPeriod2 = counts.HistoryItemCountPeriod3
				counts.HistoryItemCountPeriod3 = counts.HistoryItemCountPeriod4
				counts.HistoryItemCountPeriod4 = 0
			}
			counts.LastPeriodChangeTime += periodDurationSecs * int64(changeCount)
		}
	}
	return nil
}

// GetClientItemCount returns the stored count row for a client.
func (pg *Postgres) GetClientItemCount(ctx context.Context, clientID string) (*ClientItemCounts, error) {
	counts := &ClientItemCounts{ClientID: clientID, ID: clientID}
	row := pg.pool.QueryRow(ctx, `
SELECT item_count,
       history_item_count_period1,
       history_item_count_period2,
       history_item_count_period3,
       history_item_count_period4,
       last_period_change_time,
       version
FROM client_item_counts
WHERE client_id = $1
`, clientID)

	err := row.Scan(
		&counts.ItemCount,
		&counts.HistoryItemCountPeriod1,
		&counts.HistoryItemCountPeriod2,
		&counts.HistoryItemCountPeriod3,
		&counts.HistoryItemCountPeriod4,
		&counts.LastPeriodChangeTime,
		&counts.Version,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		now := time.Now().Unix()
		if err := pg.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM sync_entities
WHERE client_id = $1
  AND data_type NOT IN ($2, $3)
  AND deleted = FALSE
`, clientID, HistoryTypeID, HistoryDeleteDirectiveTypeID).Scan(&counts.ItemCount); err != nil {
			return nil, fmt.Errorf("error querying normal item count: %w", err)
		}
		if err := pg.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM sync_entities
WHERE client_id = $1
  AND data_type IN ($2, $3)
  AND deleted = FALSE
  AND (expiration_time IS NULL OR expiration_time > $4)
`, clientID, HistoryTypeID, HistoryDeleteDirectiveTypeID, now).Scan(&counts.HistoryItemCountPeriod4); err != nil {
			return nil, fmt.Errorf("error querying history item count: %w", err)
		}
		counts.Version = CurrentCountVersion
		counts.LastPeriodChangeTime = now
		return counts, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error getting client item counts: %w", err)
	}

	if err := pg.initRealCountsAndUpdateHistoryCounts(ctx, counts); err != nil {
		return nil, err
	}
	return counts, nil
}

// UpdateClientItemCount upserts the count row for a client.
func (pg *Postgres) UpdateClientItemCount(ctx context.Context, counts *ClientItemCounts, newNormalItemCount int, newHistoryItemCount int) error {
	counts.HistoryItemCountPeriod4 += newHistoryItemCount
	counts.ItemCount += newNormalItemCount
	if counts.Version == 0 {
		counts.Version = CurrentCountVersion
	}
	if counts.LastPeriodChangeTime == 0 {
		counts.LastPeriodChangeTime = time.Now().Unix()
	}

	_, err := pg.pool.Exec(ctx, `
INSERT INTO client_item_counts (
	client_id,
	item_count,
	history_item_count_period1,
	history_item_count_period2,
	history_item_count_period3,
	history_item_count_period4,
	last_period_change_time,
	version
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (client_id) DO UPDATE SET
	item_count = EXCLUDED.item_count,
	history_item_count_period1 = EXCLUDED.history_item_count_period1,
	history_item_count_period2 = EXCLUDED.history_item_count_period2,
	history_item_count_period3 = EXCLUDED.history_item_count_period3,
	history_item_count_period4 = EXCLUDED.history_item_count_period4,
	last_period_change_time = EXCLUDED.last_period_change_time,
	version = EXCLUDED.version
`,
		counts.ClientID,
		counts.ItemCount,
		counts.HistoryItemCountPeriod1,
		counts.HistoryItemCountPeriod2,
		counts.HistoryItemCountPeriod3,
		counts.HistoryItemCountPeriod4,
		counts.LastPeriodChangeTime,
		counts.Version,
	)
	if err != nil {
		return fmt.Errorf("error updating client item counts: %w", err)
	}
	return nil
}
