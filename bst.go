package sdq

import "iter"

// bstNode 二叉搜索树节点
type bstNode struct {
	name  string
	left  *bstNode
	right *bstNode
}

// bst 简单的二叉搜索树，用于维护有序的 topic 名称列表
type bst struct {
	root *bstNode
	size int
}

// newBST 创建新的 BST
func newBST() *bst {
	return &bst{}
}

// insert 插入节点
func (t *bst) insert(name string) {
	if t.root == nil {
		t.root = &bstNode{name: name}
		t.size++
		return
	}
	t.insertNode(t.root, name)
}

func (t *bst) insertNode(node *bstNode, name string) {
	if name < node.name {
		if node.left == nil {
			node.left = &bstNode{name: name}
			t.size++
		} else {
			t.insertNode(node.left, name)
		}
	} else if name > node.name {
		if node.right == nil {
			node.right = &bstNode{name: name}
			t.size++
		} else {
			t.insertNode(node.right, name)
		}
	}
	// name == node.name: 已存在，不插入
}

// delete 删除节点
func (t *bst) delete(name string) bool {
	if t.root == nil {
		return false
	}
	var deleted bool
	t.root, deleted = t.deleteNode(t.root, name)
	if deleted {
		t.size--
	}
	return deleted
}

func (t *bst) deleteNode(node *bstNode, name string) (*bstNode, bool) {
	if node == nil {
		return nil, false
	}

	var deleted bool
	if name < node.name {
		node.left, deleted = t.deleteNode(node.left, name)
	} else if name > node.name {
		node.right, deleted = t.deleteNode(node.right, name)
	} else {
		// 找到要删除的节点
		deleted = true

		// 情况1: 叶子节点或只有一个子节点
		if node.left == nil {
			return node.right, deleted
		}
		if node.right == nil {
			return node.left, deleted
		}

		// 情况2: 有两个子节点
		// 找到右子树的最小节点（后继节点）
		minRight := t.findMin(node.right)
		node.name = minRight.name
		node.right, _ = t.deleteNode(node.right, minRight.name)
	}

	return node, deleted
}

func (t *bst) findMin(node *bstNode) *bstNode {
	for node.left != nil {
		node = node.left
	}
	return node
}

// All 返回所有节点的迭代器（升序）
func (t *bst) All() iter.Seq[string] {
	return func(yield func(string) bool) {
		t.allTraversal(t.root, yield)
	}
}

func (t *bst) allTraversal(node *bstNode, yield func(string) bool) bool {
	if node == nil {
		return true
	}
	if !t.allTraversal(node.left, yield) {
		return false
	}
	if !yield(node.name) {
		return false
	}
	return t.allTraversal(node.right, yield)
}

// AllDesc 返回所有节点的迭代器（降序）
func (t *bst) AllDesc() iter.Seq[string] {
	return func(yield func(string) bool) {
		t.allDescTraversal(t.root, yield)
	}
}

func (t *bst) allDescTraversal(node *bstNode, yield func(string) bool) bool {
	if node == nil {
		return true
	}
	if !t.allDescTraversal(node.right, yield) {
		return false
	}
	if !yield(node.name) {
		return false
	}
	return t.allDescTraversal(node.left, yield)
}

// Range 返回指定范围的迭代器（升序，支持分页）
func (t *bst) Range(offset, limit int) iter.Seq[string] {
	return func(yield func(string) bool) {
		if offset >= t.size {
			return
		}
		count := 0
		emitted := 0
		t.rangeTraversal(t.root, offset, limit, &count, &emitted, yield)
	}
}

func (t *bst) rangeTraversal(node *bstNode, offset, limit int, count, emitted *int, yield func(string) bool) bool {
	if node == nil || *emitted >= limit {
		return false
	}

	// 遍历左子树
	if t.rangeTraversal(node.left, offset, limit, count, emitted, yield) {
		return true
	}

	// 处理当前节点
	if *count >= offset && *emitted < limit {
		if !yield(node.name) {
			return true // 提前终止
		}
		*emitted++
	}
	*count++

	if *emitted >= limit {
		return true
	}

	// 遍历右子树
	return t.rangeTraversal(node.right, offset, limit, count, emitted, yield)
}

// RangeDesc 返回指定范围的迭代器（降序，支持分页）
func (t *bst) RangeDesc(offset, limit int) iter.Seq[string] {
	return func(yield func(string) bool) {
		if offset >= t.size {
			return
		}
		count := 0
		emitted := 0
		t.rangeDescTraversal(t.root, offset, limit, &count, &emitted, yield)
	}
}

func (t *bst) rangeDescTraversal(node *bstNode, offset, limit int, count, emitted *int, yield func(string) bool) bool {
	if node == nil || *emitted >= limit {
		return false
	}

	// 遍历右子树（降序先访问右边）
	if t.rangeDescTraversal(node.right, offset, limit, count, emitted, yield) {
		return true
	}

	// 处理当前节点
	if *count >= offset && *emitted < limit {
		if !yield(node.name) {
			return true // 提前终止
		}
		*emitted++
	}
	*count++

	if *emitted >= limit {
		return true
	}

	// 遍历左子树
	return t.rangeDescTraversal(node.left, offset, limit, count, emitted, yield)
}
