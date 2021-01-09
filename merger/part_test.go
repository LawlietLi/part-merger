package merger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestPartCursorNextBeforeVal(t *testing.T) {
	part := Part{{0, 1, 2}, {3, 4}, {}, {5, 6, 7, 8}}
	cursor, _ := NewPartCursor(part)

	result := cursor.NextPartBeforeVal(6)

	assert.Equal(t, Part{{0, 1, 2}, {3, 4}, {5, 6}}, result)
}

func TestPartCursorNextBeforeValDuplicate(t *testing.T) {
	part := Part{{0, 1, 2}, {3, 4}, {5, 6, 6, 6, 7, 8}}
	cursor, _ := NewPartCursor(part)

	result := cursor.NextPartBeforeVal(6)

	assert.Equal(t, Part{{0, 1, 2}, {3, 4}, {5, 6, 6, 6}}, result)
}