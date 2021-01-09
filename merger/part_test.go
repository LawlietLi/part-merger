package merger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试游标寻找下一个
func TestPartCursorNext(t *testing.T) {
	part := Part{{0, 1, 2}, {3, 4}, {}, {5, 6, 7, 8}}
	cursor, _ := NewPartCursor(part)

	var i uint64 = 0
	for {
		assert.Equal(t, i, cursor.Val())

		i++
		_, ok := cursor.Next()
		if !ok {
			break
		}
	}
}

// 测试游标寻找某个值之前的所有数据
func TestPartCursorNextBeforeVal(t *testing.T) {
	part := Part{{0, 1, 2}, {3, 4}, {}, {5, 6, 7, 8}}
	cursor, _ := NewPartCursor(part)

	result := cursor.NextPartBeforeVal(6)

	assert.Equal(t, Part{{0, 1, 2}, {3, 4}, {5, 6}}, result)
}

// 测试带有重复数据的寻找数据情况
func TestPartCursorNextBeforeValDuplicate(t *testing.T) {
	part := Part{{0, 1, 2}, {3, 4}, {5, 6, 6, 6, 7, 8}}
	cursor, _ := NewPartCursor(part)

	result := cursor.NextPartBeforeVal(6)

	assert.Equal(t, Part{{0, 1, 2}, {3, 4}, {5, 6, 6, 6}}, result)
}

// 测试取剩余所有数据
func TestPartCursorNextAll(t *testing.T) {
	part := Part{{0, 1, 2}, {3, 4}, {5, 6, 6, 6, 7, 8, 9}, {12, 13}}
	cursor, _ := NewPartCursor(part)

	result := cursor.NextPartBeforeVal(6)
	assert.Equal(t, Part{{0, 1, 2}, {3, 4}, {5, 6, 6, 6}}, result)

	result = cursor.NextAll()
	assert.Equal(t, Part{{7, 8, 9}, {12, 13}}, result)
}
