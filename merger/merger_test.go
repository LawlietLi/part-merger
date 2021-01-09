package merger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergerMergeIntoDataBlockJumpMerge(t *testing.T) {
	part1 := Part{{1, 2}, {5, 9}}
	part2 := Part{{4, 7}, {10, 15, 16}}

	merger := BaseMerger{}
	merger.AddPart(part1, part2)

	result := merger.MergeIntoDataBlock()

	assert.Equal(t, DataBlock{1, 2, 4, 5, 7, 9, 10, 15, 16}, result)
}

func TestMergerMergeIntoDataBlockJumpMergeDuplicate(t *testing.T) {
	part1 := Part{{1, 2}, {5, 6, 6, 6, 9}}
	part2 := Part{{4, 7}, {10, 11, 11, 16}}

	merger := BaseMerger{}
	merger.AddPart(part1, part2)

	result := merger.MergeIntoDataBlock()

	assert.Equal(t, DataBlock{1, 2, 4, 5, 6, 6, 6, 7, 9, 10, 11, 11, 16}, result)
}

func TestMergerMergeIntoDataBlockHeapMerge(t *testing.T) {
	part1 := Part{{1, 2}, {5, 9}}
	part2 := Part{{4, 7}, {10, 15, 16}}
	part3 := Part{{3, 6}, {8, 11, 14}}

	merger := BaseMerger{}
	merger.AddPart(part1, part2, part3)

	result := merger.MergeIntoDataBlock()

	assert.Equal(t, DataBlock{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 16}, result)
}

func TestConcurrentMergerMergeIntoDataBlock(t *testing.T) {
	part1 := Part{{1, 2}, {5, 9}}
	part2 := Part{{4, 7}, {14, 15, 16}}
	part3 := Part{{3, 6}, {8, 10, 11, 11}}

	merger := NewConcurrentMerger(4)
	merger.AddPart(part1, part2, part3)

	result := merger.MergeIntoDataBlock()

	assert.Equal(t, DataBlock{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 11, 14, 15, 16}, result)
}

func TestConcurrentMergerFindSplitPoints(t *testing.T) {
	part1 := Part{{3, 4, 5}, {17, 18}}
	part2 := Part{{7, 8, 9, 10}}
	part3 := Part{{9, 10, 11, 12 ,13 ,14}}

	merger := NewConcurrentMerger(2)
	merger.AddPart(part1, part2, part3)

	result := merger.findSplitPoints()

	assert.Equal(t, []uint64{10}, result)

	merger = NewConcurrentMerger(4)
	merger.AddPart(part1, part2, part3)

	result = merger.findSplitPoints()

	assert.Equal(t, []uint64{5, 10 ,12}, result)
}
