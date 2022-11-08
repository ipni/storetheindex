package announce

import "container/list"

type stringLRU struct {
	cache map[string]*list.Element
	ll    *list.List
	max   int
}

func newStringLRU(maxEntries int) *stringLRU {
	return &stringLRU{
		cache: make(map[string]*list.Element, maxEntries),
		ll:    list.New(),
		max:   maxEntries,
	}
}

func (l *stringLRU) len() int {
	return l.ll.Len()
}

func (l *stringLRU) remove(s string) bool {
	if elem, hit := l.cache[s]; hit {
		l.ll.Remove(elem)
		delete(l.cache, s)
		return true
	}
	return false
}

func (l *stringLRU) update(s string) bool {
	if elem, hit := l.cache[s]; hit {
		l.ll.MoveToFront(elem)
		return true
	}

	if l.ll.Len() == l.max {
		oldest := l.ll.Back()
		l.ll.Remove(oldest)
		delete(l.cache, oldest.Value.(string))
	}
	l.cache[s] = l.ll.PushFront(s)
	return false
}
