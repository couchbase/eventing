package supervisor

import (
	"fmt"
	"maps"
	"sync"

	"github.com/couchbase/eventing/common"
)

// To play around with this data structure,
// go to :) https://go.dev/play/p/Iy04QQ8Q2Bx

type CursorTracker struct {
	num      uint8
	max      uint8
	limit    uint8
	children map[string]*CursorTracker
	funcIds  map[string]struct{}
}

type CursorRegistry struct {
	sync.RWMutex
	root *CursorTracker
}

func NewCursorRegistry(limit uint8) *CursorRegistry {
	return &CursorRegistry{
		root: &CursorTracker{
			num:      0,
			max:      0,
			limit:    limit,
			children: make(map[string]*CursorTracker),
			funcIds:  make(map[string]struct{}),
		},
	}
}

func (registry *CursorRegistry) Register(k common.KeyspaceName, funcId string) bool {
	registry.Lock()
	defer registry.Unlock()
	return registry.root.register(k, funcId)
}

func (registry *CursorRegistry) Unregister(k common.KeyspaceName, funcId string) {
	registry.Lock()
	defer registry.Unlock()
	registry.root.unregister(k, funcId)
}

func (registry *CursorRegistry) GetCursors(k common.KeyspaceName) (map[string]struct{}, bool) {
	registry.RLock()
	defer registry.RUnlock()
	return registry.root.getCursors(k)
}

func (registry *CursorRegistry) PrintTree() {
	PrintTrackerMap(registry.root, 1)
}

func (ct *CursorTracker) register(k common.KeyspaceName, funcId string) bool {
	currTracker := ct
	if _, found := currTracker.children[k.Bucket]; !found {
		currTracker.children[k.Bucket] = &CursorTracker{
			children: make(map[string]*CursorTracker),
			funcIds:  make(map[string]struct{}),
			limit:    ct.limit,
		}
	}
	if k.Scope != "*" {
		currTracker := currTracker.children[k.Bucket]
		if _, found := currTracker.children[k.Scope]; !found {
			if currTracker.num == currTracker.limit {
				return false
			}
			currTracker.children[k.Scope] = &CursorTracker{
				num:      currTracker.num,
				max:      currTracker.num,
				limit:    ct.limit,
				children: make(map[string]*CursorTracker),
				funcIds:  maps.Clone(currTracker.funcIds),
			}
		}
		if k.Collection != "*" {
			currTracker := currTracker.children[k.Scope]
			if _, found := currTracker.children[k.Collection]; !found {
				if currTracker.num == currTracker.limit {
					return false
				}
				currTracker.children[k.Collection] = &CursorTracker{
					num:      currTracker.num,
					max:      currTracker.num,
					limit:    ct.limit,
					children: make(map[string]*CursorTracker),
					funcIds:  maps.Clone(currTracker.funcIds),
				}
			}
			// Update max counters on all levels of parents
			if currTracker.children[k.Collection].max ==
				currTracker.children[k.Collection].limit {
				return false
			}
			(currTracker.children[k.Collection]).addCursor(funcId)
			if currTracker.max < currTracker.children[k.Collection].max {
				currTracker.max = currTracker.children[k.Collection].max
			}
			if ct.children[k.Bucket].max < currTracker.max {
				ct.children[k.Bucket].max = currTracker.max
			}
			if ct.max < ct.children[k.Bucket].max {
				ct.max = ct.children[k.Bucket].max
			}
		} else {
			// Update max counters on all levels of parents
			if currTracker.children[k.Scope].max ==
				currTracker.children[k.Scope].limit {
				return false
			}
			currTracker.children[k.Scope].addCursor(funcId)
			if currTracker.max < currTracker.children[k.Scope].max {
				currTracker.max = currTracker.children[k.Scope].max
			}
			if ct.max < currTracker.max {
				ct.max = currTracker.max
			}
		}
	} else {
		// Update max counters on all levels of parents
		if currTracker.children[k.Bucket].max ==
			currTracker.children[k.Bucket].limit {
			return false
		}
		(currTracker.children[k.Bucket]).addCursor(funcId)
		if currTracker.max < currTracker.children[k.Bucket].max {
			currTracker.max = currTracker.children[k.Bucket].max
		}
	}
	return true
}

func (ct *CursorTracker) unregister(k common.KeyspaceName, funcId string) {
	currTracker := ct
	if _, found := currTracker.children[k.Bucket]; !found {
		return
	}
	currTracker = currTracker.children[k.Bucket]
	if k.Scope != "*" {
		if _, found := currTracker.children[k.Scope]; !found {
			return
		}
		currTracker := currTracker.children[k.Scope]
		if k.Collection != "*" {
			if _, found := currTracker.children[k.Collection]; !found {
				return
			}
			currTracker := currTracker.children[k.Collection]
			if _, functionFound := currTracker.funcIds[funcId]; !functionFound {
				return
			}
			// perform work at collection level
			if currTracker.removeCursor(funcId) {
				delete(ct.children[k.Bucket].children[k.Scope].children, k.Collection)
			}
			ct.children[k.Bucket].children[k.Scope].reevaluatemax()
			ct.children[k.Bucket].reevaluatemax()
			ct.reevaluatemax()
			if ct.children[k.Bucket].children[k.Scope].num == 0 && len(ct.children[k.Bucket].children[k.Scope].children) == 0 {
				delete(ct.children[k.Bucket].children, k.Scope)
			}
			if ct.children[k.Bucket].num == 0 && len(ct.children[k.Bucket].children) == 0 {
				delete(ct.children, k.Bucket)
			}
			return
		}
		if _, functionFound := currTracker.funcIds[funcId]; !functionFound {
			return
		}
		// perform work at scope level
		if currTracker.removeCursor(funcId) {
			delete(ct.children[k.Bucket].children, k.Scope)
		}
		ct.children[k.Bucket].reevaluatemax()
		ct.reevaluatemax()
		if ct.children[k.Bucket].num == 0 && len(ct.children[k.Bucket].children) == 0 {
			delete(ct.children, k.Bucket)
		}
		return
	}
	if _, functionFound := currTracker.funcIds[funcId]; !functionFound {
		return
	}
	// perform work at bucket level
	if currTracker.removeCursor(funcId) {
		delete(ct.children, k.Bucket)
	}
	ct.reevaluatemax()
}

func (ct *CursorTracker) getCursors(k common.KeyspaceName) (map[string]struct{}, bool) {
	bucketComponent, bfound := ct.children[k.Bucket]
	if !bfound {
		return nil, false
	}
	scopeComponent, sfound := bucketComponent.children[k.Scope]
	if !sfound {
		return nil, false
	}
	collComponent, cfound := scopeComponent.children[k.Collection]
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

func (ct *CursorTracker) reevaluatemax() {
	max := ct.num
	for _, child := range ct.children {
		if child.max > max {
			max = child.max
		}
	}
	ct.max = max
}

func (ct *CursorTracker) removeCursor(funcId string) bool {
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

func (ct *CursorTracker) addCursor(funcId string) {
	for _, child := range ct.children {
		child.addCursor(funcId)
	}
	if _, found := ct.funcIds[funcId]; !found {
		ct.num += 1
		ct.max += 1
		ct.funcIds[funcId] = struct{}{}
	}
}

func PrintTrackerMap(tracker *CursorTracker, level int) {
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
