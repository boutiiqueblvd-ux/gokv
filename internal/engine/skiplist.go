package engine

// A skip list is the ordered half of the in-memory index. The hash map in
// keydir.go answers point lookups in O(1); this structure answers
// ReadKeyRange in O(log n + k) without ever sorting the whole key space.
//
// It is not internally synchronised: the store holds a RWMutex around the
// whole index, so there is exactly one writer at a time and readers never
// observe a partial insert.

const (
	slMaxLevel   = 20   // enough for ~2^20 keys at p=1/4
	slBranchMask = 0x03 // promote a node while the low two bits are zero
)

type slnode struct {
	key  string
	ent  entry
	next []*slnode
}

type skiplist struct {
	head  *slnode
	level int
	n     int
	rnd   uint64
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &slnode{next: make([]*slnode, slMaxLevel)},
		level: 1,
		rnd:   0x9E3779B97F4A7C15,
	}
}

// randomLevel uses a private xorshift generator so index writes never contend
// on the global math/rand lock.
func (s *skiplist) randomLevel() int {
	s.rnd ^= s.rnd << 13
	s.rnd ^= s.rnd >> 7
	s.rnd ^= s.rnd << 17
	x := s.rnd
	lvl := 1
	for lvl < slMaxLevel && x&slBranchMask == 0 {
		lvl++
		x >>= 2
	}
	return lvl
}

func (s *skiplist) Len() int { return s.n }

// insert adds key, or overwrites the entry of an existing node. It returns the
// node so the caller can keep a direct pointer to it in the hash index.
func (s *skiplist) insert(key string, e entry) *slnode {
	var prev [slMaxLevel]*slnode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		prev[i] = x
	}
	if next := x.next[0]; next != nil && next.key == key {
		next.ent = e
		return next
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			prev[i] = s.head
		}
		s.level = lvl
	}
	n := &slnode{key: key, ent: e, next: make([]*slnode, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	s.n++
	return n
}

func (s *skiplist) remove(key string) bool {
	var prev [slMaxLevel]*slnode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		prev[i] = x
	}
	target := x.next[0]
	if target == nil || target.key != key {
		return false
	}
	for i := 0; i < s.level; i++ {
		if prev[i].next[i] == target {
			prev[i].next[i] = target.next[i]
		}
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
	s.n--
	return true
}

// seek returns the first node whose key is >= key.
func (s *skiplist) seek(key string) *slnode {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	return x.next[0]
}

// scan visits every node in [start, end] in key order. An empty end means
// "to the last key". fn returns false to stop early.
func (s *skiplist) scan(start, end string, fn func(n *slnode) bool) {
	for n := s.seek(start); n != nil; n = n.next[0] {
		if end != "" && n.key > end {
			return
		}
		if !fn(n) {
			return
		}
	}
}
