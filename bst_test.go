package sdq

import (
	"reflect"
	"testing"
)

// 测试辅助方法（仅在测试中使用，不会被编译到生产代码）

// get 查找节点是否存在
func (t *bst) get(name string) bool {
	return t.getNode(t.root, name) != nil
}

func (t *bst) getNode(node *bstNode, name string) *bstNode {
	if node == nil {
		return nil
	}
	if name < node.name {
		return t.getNode(node.left, name)
	} else if name > node.name {
		return t.getNode(node.right, name)
	}
	return node
}

// 测试用例

func TestBST_InsertAndGet(t *testing.T) {
	tree := newBST()

	// 插入节点
	tree.insert("topic-b")
	tree.insert("topic-a")
	tree.insert("topic-c")

	// 验证存在
	if !tree.get("topic-a") {
		t.Error("topic-a should exist")
	}
	if !tree.get("topic-b") {
		t.Error("topic-b should exist")
	}
	if !tree.get("topic-c") {
		t.Error("topic-c should exist")
	}

	// 验证不存在
	if tree.get("topic-d") {
		t.Error("topic-d should not exist")
	}

	// 验证大小
	if tree.size != 3 {
		t.Errorf("expected size 3, got %d", tree.size)
	}
}

func TestBST_InsertDuplicate(t *testing.T) {
	tree := newBST()

	tree.insert("topic-a")
	tree.insert("topic-a")

	if tree.size != 1 {
		t.Errorf("duplicate insert should not increase size, got %d", tree.size)
	}
}

func TestBST_Delete(t *testing.T) {
	tree := newBST()

	// 插入节点
	tree.insert("topic-b")
	tree.insert("topic-a")
	tree.insert("topic-c")
	tree.insert("topic-d")

	// 删除存在的节点
	if !tree.delete("topic-b") {
		t.Error("delete should return true for existing node")
	}

	if tree.get("topic-b") {
		t.Error("topic-b should be deleted")
	}

	if tree.size != 3 {
		t.Errorf("expected size 3 after delete, got %d", tree.size)
	}

	// 删除不存在的节点
	if tree.delete("topic-x") {
		t.Error("delete should return false for non-existing node")
	}

	if tree.size != 3 {
		t.Errorf("size should not change for failed delete, got %d", tree.size)
	}
}

func TestBST_InorderAscending(t *testing.T) {
	tree := newBST()

	// 乱序插入
	tree.insert("topic-d")
	tree.insert("topic-b")
	tree.insert("topic-a")
	tree.insert("topic-c")
	tree.insert("topic-e")

	expected := []string{"topic-a", "topic-b", "topic-c", "topic-d", "topic-e"}
	var result []string
	for name := range tree.All() {
		result = append(result, name)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestBST_InorderDescending(t *testing.T) {
	tree := newBST()

	// 乱序插入
	tree.insert("topic-d")
	tree.insert("topic-b")
	tree.insert("topic-a")
	tree.insert("topic-c")
	tree.insert("topic-e")

	expected := []string{"topic-e", "topic-d", "topic-c", "topic-b", "topic-a"}
	var result []string
	for name := range tree.AllDesc() {
		result = append(result, name)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestBST_InorderRangeAscending(t *testing.T) {
	tree := newBST()

	// 插入 10 个节点
	for i := 0; i < 10; i++ {
		tree.insert(string(rune('a' + i)))
	}

	tests := []struct {
		offset   int
		limit    int
		expected []string
	}{
		{0, 3, []string{"a", "b", "c"}},
		{3, 3, []string{"d", "e", "f"}},
		{7, 5, []string{"h", "i", "j"}},
	}

	for _, tt := range tests {
		var result []string
		for name := range tree.Range(tt.offset, tt.limit) {
			result = append(result, name)
		}
		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("Range(%d, %d) = %v, want %v", tt.offset, tt.limit, result, tt.expected)
		}
	}

	// 测试 offset >= size
	var emptyResult []string
	for name := range tree.Range(10, 5) {
		emptyResult = append(emptyResult, name)
	}
	if len(emptyResult) != 0 {
		t.Errorf("Range(10, 5) should return no elements, got %v", emptyResult)
	}
}

func TestBST_InorderRangeDescending(t *testing.T) {
	tree := newBST()

	// 插入 10 个节点
	for i := 0; i < 10; i++ {
		tree.insert(string(rune('a' + i)))
	}

	tests := []struct {
		offset   int
		limit    int
		expected []string
	}{
		{0, 3, []string{"j", "i", "h"}},
		{3, 3, []string{"g", "f", "e"}},
		{7, 5, []string{"c", "b", "a"}},
	}

	for _, tt := range tests {
		var result []string
		for name := range tree.RangeDesc(tt.offset, tt.limit) {
			result = append(result, name)
		}
		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("RangeDesc(%d, %d) = %v, want %v", tt.offset, tt.limit, result, tt.expected)
		}
	}

	// 测试 offset >= size
	var emptyResult []string
	for name := range tree.RangeDesc(10, 5) {
		emptyResult = append(emptyResult, name)
	}
	if len(emptyResult) != 0 {
		t.Errorf("RangeDesc(10, 5) should return no elements, got %v", emptyResult)
	}
}

func TestBST_Empty(t *testing.T) {
	tree := newBST()

	if tree.size != 0 {
		t.Errorf("new tree should have size 0, got %d", tree.size)
	}

	if tree.get("anything") {
		t.Error("empty tree should not contain any items")
	}

	var result []string
	for name := range tree.All() {
		result = append(result, name)
	}
	if len(result) != 0 {
		t.Errorf("empty tree All() should return nothing, got %v", result)
	}

	if tree.delete("anything") {
		t.Error("delete from empty tree should return false")
	}
}

func TestBST_DeleteWithTwoChildren(t *testing.T) {
	tree := newBST()

	// 创建一个有两个子节点的节点
	//       d
	//      / \
	//     b   f
	//    / \ / \
	//   a  c e  g
	tree.insert("d")
	tree.insert("b")
	tree.insert("f")
	tree.insert("a")
	tree.insert("c")
	tree.insert("e")
	tree.insert("g")

	// 删除有两个子节点的根节点
	tree.delete("d")

	// 验证顺序仍然正确
	expected := []string{"a", "b", "c", "e", "f", "g"}
	var result []string
	for name := range tree.All() {
		result = append(result, name)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("after deleting node with two children, expected %v, got %v", expected, result)
	}

	if tree.size != 6 {
		t.Errorf("expected size 6 after delete, got %d", tree.size)
	}
}

func TestBST_IteratorAll(t *testing.T) {
	tree := newBST()

	// 插入节点
	tree.insert("c")
	tree.insert("a")
	tree.insert("b")
	tree.insert("e")
	tree.insert("d")

	// 测试升序迭代器
	var result []string
	for name := range tree.All() {
		result = append(result, name)
	}
	expected := []string{"a", "b", "c", "d", "e"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("All() = %v, want %v", result, expected)
	}

	// 测试降序迭代器
	result = nil
	for name := range tree.AllDesc() {
		result = append(result, name)
	}
	expectedDesc := []string{"e", "d", "c", "b", "a"}
	if !reflect.DeepEqual(result, expectedDesc) {
		t.Errorf("AllDesc() = %v, want %v", result, expectedDesc)
	}
}

func TestBST_IteratorRange(t *testing.T) {
	tree := newBST()

	// 插入 10 个节点
	for i := 0; i < 10; i++ {
		tree.insert(string(rune('a' + i)))
	}

	// 测试升序范围迭代器
	var result []string
	for name := range tree.Range(3, 4) {
		result = append(result, name)
	}
	expected := []string{"d", "e", "f", "g"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Range(3, 4) = %v, want %v", result, expected)
	}

	// 测试降序范围迭代器
	result = nil
	for name := range tree.RangeDesc(2, 3) {
		result = append(result, name)
	}
	expectedDesc := []string{"h", "g", "f"}
	if !reflect.DeepEqual(result, expectedDesc) {
		t.Errorf("RangeDesc(2, 3) = %v, want %v", result, expectedDesc)
	}
}

func TestBST_IteratorEarlyTermination(t *testing.T) {
	tree := newBST()

	// 插入节点
	for i := 0; i < 10; i++ {
		tree.insert(string(rune('a' + i)))
	}

	// 测试提前终止
	var result []string
	for name := range tree.All() {
		result = append(result, name)
		if len(result) == 3 {
			break // 提前终止
		}
	}

	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("early termination = %v, want %v", result, expected)
	}
}
