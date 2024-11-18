package supervisor2

import (
	"fmt"
	"maps"
	"sync"

	"github.com/couchbase/eventing/application"
)

// To play around with this data structure,
// go to :) https://go.dev/play/p/Iy04QQ8Q2Bx

type cursorTracker struct {
	num      uint8
	max      uint8
	limit    uint8
	children map[string]*cursorTracker
	funcIds  map[string]struct{}
}

// TODO: Make this lock free or high read and rare write kind of datastructure
type cursorRegistry struct {
	sync.RWMutex
	root *cursorTracker
}

func NewCursorRegistry(limit uint8) *cursorRegistry {
	return &cursorRegistry{
		root: &cursorTracker{
			num:      0,
			max:      0,
			limit:    limit,
			children: make(map[string]*cursorTracker),
			funcIds:  make(map[string]struct{}),
		},
	}
}

func (registry *cursorRegistry) UpdateLimit(newlimit uint8) {
	registry.Lock()
	defer registry.Unlock()
	traverse(registry.root, 0, func(tracker *cursorTracker, level int) {
		if tracker.limit != newlimit {
			tracker.limit = newlimit
		}
	})
}

func (registry *cursorRegistry) Register(k application.Keyspace, funcId string) bool {
	registry.Lock()
	defer registry.Unlock()
	return registry.root.checkOrUpdate(k, funcId, true)
}

func (registry *cursorRegistry) IsRegisterPossible(k application.Keyspace, funcId string) bool {
	registry.Lock()
	defer registry.RUnlock()

	return registry.root.checkOrUpdate(k, funcId, false)
}

func (registry *cursorRegistry) Unregister(k application.Keyspace, funcId string) {
	registry.Lock()
	defer registry.Unlock()
	registry.root.unregister(k, funcId)
}

func (registry *cursorRegistry) GetCursors(k application.Keyspace) (map[string]struct{}, bool) {
	registry.RLock()
	defer registry.RUnlock()
	return registry.root.getCursors(k)
}

func (registry *cursorRegistry) PrintTree() {
	PrintTrackerMap(registry.root, 1)
}

func (ct *cursorTracker) checkOrUpdate(k application.Keyspace, funcId string, update bool) bool {
	currTracker := ct
	if _, found := currTracker.children[k.BucketName]; !found {
		if !update {
			return true
		}
		currTracker.children[k.BucketName] = &cursorTracker{
			children: make(map[string]*cursorTracker),
			funcIds:  make(map[string]struct{}),
			limit:    ct.limit,
		}
	}
	if k.ScopeName != "*" {
		currTracker := currTracker.children[k.BucketName]
		if _, found := currTracker.children[k.ScopeName]; !found {
			if currTracker.num == currTracker.limit {
				return false
			}
			if update {
				currTracker.children[k.ScopeName] = &cursorTracker{
					num:      currTracker.num,
					max:      currTracker.num,
					limit:    ct.limit,
					children: make(map[string]*cursorTracker),
					funcIds:  maps.Clone(currTracker.funcIds),
				}
			}
		}
		if k.CollectionName != "*" {
			currTracker := currTracker.children[k.ScopeName]
			if _, found := currTracker.children[k.CollectionName]; !found {
				if currTracker.num == currTracker.limit {
					return false
				}
				if update {
					currTracker.children[k.CollectionName] = &cursorTracker{
						num:      currTracker.num,
						max:      currTracker.num,
						limit:    ct.limit,
						children: make(map[string]*cursorTracker),
						funcIds:  maps.Clone(currTracker.funcIds),
					}
				}
			}
			// Update max counters on all levels of parents
			if currTracker.children[k.CollectionName].max ==
				currTracker.children[k.CollectionName].limit {
				return false
			}
			if update {
				(currTracker.children[k.CollectionName]).addCursor(funcId)

				if currTracker.max < currTracker.children[k.CollectionName].max {
					currTracker.max = currTracker.children[k.CollectionName].max
				}
				if ct.children[k.BucketName].max < currTracker.max {
					ct.children[k.BucketName].max = currTracker.max
				}
				if ct.max < ct.children[k.BucketName].max {
					ct.max = ct.children[k.BucketName].max
				}
			}
		} else {
			// Update max counters on all levels of parents
			if currTracker.children[k.ScopeName].max ==
				currTracker.children[k.ScopeName].limit {
				return false
			}
			if update {
				currTracker.children[k.ScopeName].addCursor(funcId)
				if currTracker.max < currTracker.children[k.ScopeName].max {
					currTracker.max = currTracker.children[k.ScopeName].max
				}
				if ct.max < currTracker.max {
					ct.max = currTracker.max
				}
			}
		}
	} else {
		// Update max counters on all levels of parents
		if currTracker.children[k.BucketName].max ==
			currTracker.children[k.BucketName].limit {
			return false
		}
		if update {
			(currTracker.children[k.BucketName]).addCursor(funcId)
			if currTracker.max < currTracker.children[k.BucketName].max {
				currTracker.max = currTracker.children[k.BucketName].max
			}
		}
	}
	return true
}

func (ct *cursorTracker) unregister(k application.Keyspace, funcId string) {
	currTracker := ct
	if _, found := currTracker.children[k.BucketName]; !found {
		return
	}
	currTracker = currTracker.children[k.BucketName]
	if k.ScopeName != "*" {
		if _, found := currTracker.children[k.ScopeName]; !found {
			return
		}
		currTracker := currTracker.children[k.ScopeName]
		if k.CollectionName != "*" {
			if _, found := currTracker.children[k.CollectionName]; !found {
				return
			}
			currTracker := currTracker.children[k.CollectionName]
			if _, functionFound := currTracker.funcIds[funcId]; !functionFound {
				return
			}
			// perform work at collection level
			if currTracker.removeCursor(funcId) {
				delete(ct.children[k.BucketName].children[k.ScopeName].children, k.CollectionName)
			}
			ct.children[k.BucketName].children[k.ScopeName].reevaluatemax()
			ct.children[k.BucketName].reevaluatemax()
			ct.reevaluatemax()
			if ct.children[k.BucketName].children[k.ScopeName].num == 0 && len(ct.children[k.BucketName].children[k.ScopeName].children) == 0 {
				delete(ct.children[k.BucketName].children, k.ScopeName)
			}
			if ct.children[k.BucketName].num == 0 && len(ct.children[k.BucketName].children) == 0 {
				delete(ct.children, k.BucketName)
			}
			return
		}
		if _, functionFound := currTracker.funcIds[funcId]; !functionFound {
			return
		}
		// perform work at scope level
		if currTracker.removeCursor(funcId) {
			delete(ct.children[k.BucketName].children, k.ScopeName)
		}
		ct.children[k.BucketName].reevaluatemax()
		ct.reevaluatemax()
		if ct.children[k.BucketName].num == 0 && len(ct.children[k.BucketName].children) == 0 {
			delete(ct.children, k.BucketName)
		}
		return
	}
	if _, functionFound := currTracker.funcIds[funcId]; !functionFound {
		return
	}
	// perform work at bucket level
	if currTracker.removeCursor(funcId) {
		delete(ct.children, k.BucketName)
	}
	ct.reevaluatemax()
}

func (ct *cursorTracker) getCursors(k application.Keyspace) (map[string]struct{}, bool) {
	bucketComponent, bfound := ct.children[k.BucketName]
	if !bfound {
		return nil, false
	}
	scopeComponent, sfound := bucketComponent.children[k.ScopeName]
	if !sfound {
		return nil, false
	}
	collComponent, cfound := scopeComponent.children[k.CollectionName]
	if !cfound {
		return nil, false
	}
	if collComponent.num == 0 {
		return nil, true
	}
	cursors := make(map[string]struct{}, collComponent.num)
	for key, _ := range collComponent.funcIds {
		cursors[key] = struct{}{}
	}
	return cursors, true
}

func (ct *cursorTracker) reevaluatemax() {
	max := ct.num
	for _, child := range ct.children {
		if child.max > max {
			max = child.max
		}
	}
	ct.max = max
}

func (ct *cursorTracker) removeCursor(funcId string) bool {
	for componentId, child := range ct.children {
		if child.removeCursor(funcId) {
			delete(ct.children, componentId)
		}
	}
	ct.num -= 1
	ct.max -= 1
	delete(ct.funcIds, funcId)
	if ct.num == 0 && len(ct.children) == 0 {
		return true
	}
	return false
}

func (ct *cursorTracker) addCursor(funcId string) {
	for _, child := range ct.children {
		child.addCursor(funcId)
	}
	if _, found := ct.funcIds[funcId]; !found {
		ct.num += 1
		ct.max += 1
		ct.funcIds[funcId] = struct{}{}
	}
}

func traverse(tracker *cursorTracker, level int, cb func(tracker *cursorTracker, level int)) {
	cb(tracker, level)
	for _, child := range tracker.children {
		traverse(child, level+1, cb)
	}
}

func PrintTrackerMap(tracker *cursorTracker, level int) {
	for i := 0; i < level; i++ {
		fmt.Printf("\t")
	}
	fmt.Printf("num: %d\n", tracker.num)
	for i := 0; i < level; i++ {
		fmt.Printf("\t")
	}
	fmt.Printf("max: %d\n", tracker.max)
	for i := 0; i < level; i++ {
		fmt.Printf("\t")
	}
	fmt.Printf("funcIds: %v\n", tracker.funcIds)
	for i := 0; i < level; i++ {
		fmt.Printf("\t")
	}
	fmt.Printf("children:\n")
	for componentName, child := range tracker.children {
		for i := 0; i < level; i++ {
			fmt.Printf("\t")
		}
		fmt.Printf("%s\n", componentName)
		PrintTrackerMap(child, level+1)
	}
}
