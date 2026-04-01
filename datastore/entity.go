package datastore

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/brave/go-sync/schema/protobuf/sync_pb"
)

const (
	reasonDeleted                    = "deleted"
	HistoryTypeID                int = 963985
	HistoryDeleteDirectiveTypeID int = 150251
	// Expiration time for history and history delete directive
	// entities in seconds.
	HistoryExpirationIntervalSecs = 14 * 24 * 60 * 60 // 14 days
)

// SyncEntity is the datastore representation of a sync item.
type SyncEntity struct {
	ClientID               string
	ID                     string
	ParentID               *string
	Version                *int64
	Mtime                  *int64
	Ctime                  *int64
	Name                   *string
	NonUniqueName          *string
	ServerDefinedUniqueTag *string
	Deleted                *bool
	OriginatorCacheGUID    *string
	OriginatorClientItemID *string
	Specifics              []byte
	DataType               *int
	Folder                 *bool
	ClientDefinedUniqueTag *string
	UniquePosition         []byte
	DataTypeMtime          *string
	ExpirationTime         *int64
}

// SyncEntityByClientIDID implements sort.Interface for []SyncEntity based on
// the string concatenation of ClientID and ID fields.
type SyncEntityByClientIDID []SyncEntity

func (a SyncEntityByClientIDID) Len() int      { return len(a) }
func (a SyncEntityByClientIDID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SyncEntityByClientIDID) Less(i, j int) bool {
	return a[i].ClientID+a[i].ID < a[j].ClientID+a[j].ID
}

// SyncEntityByMtime implements sort.Interface for []SyncEntity based on Mtime.
type SyncEntityByMtime []SyncEntity

func (a SyncEntityByMtime) Len() int      { return len(a) }
func (a SyncEntityByMtime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SyncEntityByMtime) Less(i, j int) bool {
	return *a[i].Mtime < *a[j].Mtime
}

func validatePBEntity(entity *sync_pb.SyncEntity) error {
	if entity == nil {
		return errors.New("validate SyncEntity error: empty SyncEntity")
	}

	if entity.IdString == nil {
		return errors.New("validate SyncEntity error: empty IdString")
	}

	if entity.Version == nil {
		return errors.New("validate SyncEntity error: empty Version")
	}

	if entity.Specifics == nil {
		return errors.New("validate SyncEntity error: nil Specifics")
	}

	return nil
}

// CreateDBSyncEntity converts a protobuf sync entity into a DB sync item.
func CreateDBSyncEntity(entity *sync_pb.SyncEntity, cacheGUID *string, clientID string) (*SyncEntity, error) {
	err := validatePBEntity(entity)
	if err != nil {
		log.Error().Err(err).Msg("Invalid sync_pb.SyncEntity received")
		return nil, fmt.Errorf("error validating protobuf sync entity to create DB sync entity: %w", err)
	}

	// Specifics are always passed and checked by validatePBEntity above.
	specifics, err := proto.Marshal(entity.Specifics)
	if err != nil {
		log.Error().Err(err).Msg("Marshal specifics failed")
		return nil, fmt.Errorf("error marshalling specifics to create DB sync entity: %w", err)
	}

	// Use reflect to find out data type ID defined in protobuf tag.
	structField := reflect.ValueOf(entity.Specifics.SpecificsVariant).Elem().Type().Field(0)
	tag := structField.Tag.Get("protobuf")
	s := strings.Split(tag, ",")
	dataType, _ := strconv.Atoi(s[1])

	var uniquePosition []byte
	if entity.UniquePosition != nil {
		uniquePosition, err = proto.Marshal(entity.UniquePosition)
		if err != nil {
			log.Error().Err(err).Msg("Marshal UniquePosition failed")
			return nil, fmt.Errorf("error marshalling unique position to create DB sync entity: %w", err)
		}
	}

	id := *entity.IdString
	var originatorCacheGUID, originatorClientItemID *string
	if cacheGUID != nil {
		if *entity.Version == 0 {
			id = uuid.New().String()
		}
		originatorCacheGUID = cacheGUID
		originatorClientItemID = entity.IdString
	}

	// The client tag hash must be used as the primary key for the history type.
	if dataType == HistoryTypeID {
		id = *entity.ClientTagHash
	}

	now := time.Now()

	var expirationTime *int64
	if dataType == HistoryTypeID || dataType == HistoryDeleteDirectiveTypeID {
		expiresAt := now.Unix() + HistoryExpirationIntervalSecs
		expirationTime = int64Ptr(expiresAt)
	}

	nowMillis := int64Ptr(now.UnixMilli())
	// ctime is only used when inserting a new entity, here we use client passed
	// ctime if it is passed, otherwise, use current server time as the creation
	// time. When updating, ctime will be ignored later in the query statement.
	cTime := nowMillis
	if entity.Ctime != nil {
		cTime = entity.Ctime
	}

	dataTypeMtime := strconv.Itoa(dataType) + "#" + strconv.FormatInt(*nowMillis, 10)

	// Set default values on Deleted and Folder attributes for new entities, the
	// default values are specified by sync.proto protocol.
	deleted := entity.Deleted
	folder := entity.Folder
	if *entity.Version == 0 {
		if entity.Deleted == nil {
			deleted = boolPtr(false)
		}
		if entity.Folder == nil {
			folder = boolPtr(false)
		}
	}

	return &SyncEntity{
		ClientID:               clientID,
		ID:                     id,
		ParentID:               entity.ParentIdString,
		Version:                entity.Version,
		Ctime:                  cTime,
		Mtime:                  nowMillis,
		Name:                   entity.Name,
		NonUniqueName:          entity.NonUniqueName,
		ServerDefinedUniqueTag: entity.ServerDefinedUniqueTag,
		Deleted:                deleted,
		OriginatorCacheGUID:    originatorCacheGUID,
		OriginatorClientItemID: originatorClientItemID,
		ClientDefinedUniqueTag: entity.ClientTagHash,
		Specifics:              specifics,
		Folder:                 folder,
		UniquePosition:         uniquePosition,
		DataType:               intPtr(dataType),
		DataTypeMtime:          stringPtr(dataTypeMtime),
		ExpirationTime:         expirationTime,
	}, nil
}

// CreatePBSyncEntity converts a DB sync item to a protobuf sync entity.
func CreatePBSyncEntity(entity *SyncEntity) (*sync_pb.SyncEntity, error) {
	pbEntity := &sync_pb.SyncEntity{
		IdString:               &entity.ID,
		ParentIdString:         entity.ParentID,
		Version:                entity.Version,
		Mtime:                  entity.Mtime,
		Ctime:                  entity.Ctime,
		Name:                   entity.Name,
		NonUniqueName:          entity.NonUniqueName,
		ServerDefinedUniqueTag: entity.ServerDefinedUniqueTag,
		ClientTagHash:          entity.ClientDefinedUniqueTag,
		OriginatorCacheGuid:    entity.OriginatorCacheGUID,
		OriginatorClientItemId: entity.OriginatorClientItemID,
		Deleted:                entity.Deleted,
		Folder:                 entity.Folder,
	}

	if entity.Specifics != nil {
		pbEntity.Specifics = &sync_pb.EntitySpecifics{}
		err := proto.Unmarshal(entity.Specifics, pbEntity.Specifics)
		if err != nil {
			log.Error().Err(err).Msg("Unmarshal specifics failed")
			return nil, fmt.Errorf("error unmarshalling specifics to create protobuf sync entity: %w", err)
		}
	}

	if entity.UniquePosition != nil {
		pbEntity.UniquePosition = &sync_pb.UniquePosition{}
		err := proto.Unmarshal(entity.UniquePosition, pbEntity.UniquePosition)
		if err != nil {
			log.Error().Err(err).Msg("Unmarshal UniquePosition failed")
			return nil, fmt.Errorf("error unmarshalling unique position to create protobuf sync entity: %w", err)
		}
	}

	return pbEntity, nil
}

func boolPtr(v bool) *bool { return &v }

func int64Ptr(v int64) *int64 { return &v }

func intPtr(v int) *int { return &v }

func stringPtr(v string) *string { return &v }

var (
	_ sort.Interface = SyncEntityByClientIDID{}
	_ sort.Interface = SyncEntityByMtime{}
)
