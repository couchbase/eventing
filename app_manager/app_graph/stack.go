package appGraph

const (
	defaultCapacity = 4
)

type Stack[T any] struct {
	items []T
}

func NewStack[T any]() *Stack[T] {
	return &Stack[T]{items: make([]T, 0, defaultCapacity)}
}

func (st *Stack[T]) Size() int {
	return st.size()
}

func (st *Stack[T]) Top() (defaultValue T, found bool) {
	if st.size() == 0 {
		return
	}
	return st.items[len(st.items)-1], true
}

func (st *Stack[T]) Pop() (defaultValue T, found bool) {
	if st.size() == 0 {
		return
	}

	elem := st.items[len(st.items)-1]
	st.items = st.items[:len(st.items)-1]
	return elem, true
}

func (st *Stack[T]) Push(value T) {
	st.items = append(st.items, value)
}

func (st *Stack[T]) size() int {
	return len(st.items)
}
