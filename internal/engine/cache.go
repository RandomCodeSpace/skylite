package engine

import (
	"container/list"
	"sync"
)

// Cache holds verified page bodies fetched from storage, up to a fixed
// number of bytes, and evicts the least recently used. Entries are keyed by
// the immutable identity of what was fetched (object key, version token and
// offset), so a hit is always the bytes that object version holds there;
// which object a page is read from is decided by the reader, never by the
// cache. Correctness never depends on it. A Cache is safe for concurrent
// use, and its lock is never held while storage is read.
type Cache struct {
	mu     sync.Mutex
	budget int64
	used   int64
	ll     *list.List // front is most recently used
	m      map[string]*list.Element
}

type cacheEntry struct {
	key  string
	body []byte
}

// NewCache returns a cache that holds at most budget bytes of keys and page
// bodies. A budget of zero or less caches nothing.
func NewCache(budget int64) *Cache {
	return &Cache{budget: budget, ll: list.New(), m: make(map[string]*list.Element)}
}

func (c *Cache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(e)
	return e.Value.(*cacheEntry).body, true
}

// put stores body, which the caller must not modify afterwards.
func (c *Cache) put(key string, body []byte) {
	size := int64(len(key) + len(body))
	c.mu.Lock()
	defer c.mu.Unlock()
	if size > c.budget {
		return
	}
	if _, ok := c.m[key]; ok {
		return
	}
	c.m[key] = c.ll.PushFront(&cacheEntry{key: key, body: body})
	c.used += size
	for c.used > c.budget {
		e := c.ll.Back()
		ce := e.Value.(*cacheEntry)
		c.ll.Remove(e)
		delete(c.m, ce.key)
		c.used -= int64(len(ce.key) + len(ce.body))
	}
}
