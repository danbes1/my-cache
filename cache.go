package main

import (
	"context"
	"sync"
	"time"
)

type Cache interface {
	Set(key string, value any)
	Get(key string) (any, bool)
	Delete(key string)
	Flush()
	Close()
}

const ttl = 5 * time.Minute

type cacheItem struct {
	value     any
	expiresAt time.Time
}

type MyCache struct {
	elems  map[string]cacheItem
	mu     sync.RWMutex
	cancel context.CancelFunc
}

func (c *MyCache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.elems[key] = cacheItem{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

func (c *MyCache) Get(key string) (any, bool) {
	c.mu.RLock()

	res, ok := c.elems[key]
	c.mu.RUnlock() // снимаю блокировку, чтобы не получить дедлок для "мёртвого" ключа

	if !ok {
		return nil, false
	}

	if time.Now().After(res.expiresAt) {
		c.Delete(key)
		return nil, false
	}

	return res.value, ok
}

func (c *MyCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.elems, key)
}

func (c *MyCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.elems {
		delete(c.elems, i)
	}
}

func (c *MyCache) Close() {
	if c.cancel != nil {
		c.cancel()
	}

}

func (c *MyCache) startGc(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for key, item := range c.elems {
				if now.After(item.expiresAt) {
					delete(c.elems, key)
				}
			}
			c.mu.Unlock()

		case <-ctx.Done():
			return
		}

	}
}

func NewMyCache(gcInterval time.Duration) Cache {
	ctx, cancel := context.WithCancel(context.Background())
	cache := &MyCache{
		elems:  make(map[string]cacheItem),
		cancel: cancel,
	}

	go cache.startGc(ctx, gcInterval)
	return cache
}
