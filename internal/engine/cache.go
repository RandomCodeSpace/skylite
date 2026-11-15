package engine

import (
	"container/list"
	"sync"
)

// Cache holds verified page bodies fetched from storage, up to a fixed
// number of bytes of heap, and evicts the least recently used. Entries are
// keyed by the immutable identity of what was fetched (object key, version
// token and offset), so a hit is always the bytes that object version holds
// there; which object a page is read from is decided by the reader, never by
// the cache. Correctness never depends on it. A Cache is safe for concurrent
// use, and its lock is never held while storage is read. A nil *Cache caches
// nothing.
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

// entryOverhead is the heap an entry takes besides its key and body: its
// list element, its cacheEntry and its map slot. TestCacheHeap measures it.
const entryOverhead = 160

func entrySize(key string, body []byte) int64 { return int64(len(key)+len(body)) + entryOverhead }

// NewCache returns a cache that holds at most budget bytes: keys, page
// bodies and a fixed overhead per entry. A budget of zero or less caches
// nothing.
func NewCache(budget int64) *Cache {
	return &Cache{budget: budget, ll: list.New(), m: make(map[string]*list.Element)}
}

func (c *Cache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(e)
	return e.Value.(*cacheEntry).body, true
}

// put stores a copy of body, so that the cache holds exactly its bytes.
func (c *Cache) put(key string, body []byte) {
	if c == nil {
		return
	}
	size := entrySize(key, body)
	if size > c.budget {
		return
	}
	body = append([]byte(nil), body...)
	c.mu.Lock()
	defer c.mu.Unlock()
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
		c.used -= entrySize(ce.key, ce.body)
	}
}
