package util

type node struct {
	data interface{}
	next *node
}

type Stack struct {
	top  *node
	size int
}

func NewStack() *Stack {
	return &Stack{nil, 0}
}

func (st *Stack) Size() int {
	return st.size
}

func (st *Stack) Top() interface{} {
	if st.size == 0 {
		return nil
	}
	return st.top.data
}

func (st *Stack) Pop() interface{} {
	if st.size == 0 {
		return nil
	}

	elem := st.top
	st.top = elem.next
	st.size--
	return elem.data
}

func (st *Stack) Push(value interface{}) {
	elem := &node{value, st.top}
	st.top = elem
	st.size++
}
