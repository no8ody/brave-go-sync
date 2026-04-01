package cache

import (
	"context"
	"strconv"
	"sync"
	"time"
)

type memoryValue struct {
	value     string
	expiresAt time.Time
}

type memoryClient struct {
	mu    sync.RWMutex
	items map[string]memoryValue
}

// NewMemoryClient creates an in-memory cache backend. It is intended for
// self-hosted and single-instance deployments where running Redis is not
// desired.
func NewMemoryClient() RedisClient {
	return &memoryClient{items: map[string]memoryValue{}}
}

func (m *memoryClient) Set(_ context.Context, key string, val string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	item := memoryValue{value: val}
	if ttl > 0 {
		item.expiresAt = time.Now().Add(ttl)
	}
	m.items[key] = item
	return nil
}

func (m *memoryClient) Incr(_ context.Context, key string, subtract bool) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := 0
	if item, ok := m.items[key]; ok && !item.isExpired() {
		parsed, err := strconv.Atoi(item.value)
		if err != nil {
			return 0, err
		}
		current = parsed
	}

	if subtract {
		current--
	} else {
		current++
	}

	m.items[key] = memoryValue{value: strconv.Itoa(current)}
	return current, nil
}

func (m *memoryClient) Get(_ context.Context, key string, deleteAfterGet bool) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item, ok := m.items[key]
	if !ok || item.isExpired() {
		delete(m.items, key)
		return "", nil
	}
	if deleteAfterGet {
		delete(m.items, key)
	}
	return item.value, nil
}

func (m *memoryClient) Del(_ context.Context, keys ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		delete(m.items, key)
	}
	return nil
}

func (m *memoryClient) FlushAll(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items = map[string]memoryValue{}
	return nil
}

func (v memoryValue) isExpired() bool {
	return !v.expiresAt.IsZero() && time.Now().After(v.expiresAt)
}
