package merger

import (
	"container/heap"
	"math"
	"sync"
)

// PartMerger 分区合并器接口
type PartMerger interface {
	AddPart(parts... Part)          // 添加分区
	MergeIntoDataBlock() DataBlock  // 执行合并
}

// BaseMerger 分区合并器
type BaseMerger struct {
	parts    []Part         // 需要合并的分区
	partHeap PartCursorHeap // 分区游标堆
	result   DataBlock      // 合并结果
}

// AddPart 添加要合并的分区
// 返回合并器指针，方便链式调用
func (m *BaseMerger) AddPart(parts... Part) {
	m.parts = append(m.parts, parts...)
}

// MergeIntoDataBlock 合并分区到一个数据块中
func (m *BaseMerger) MergeIntoDataBlock() DataBlock {
	m.partHeap = PartCursorHeap{}
	// 初始化所有需要合并分区的游标
	for _, part := range m.parts {
		cursor, ok := NewPartCursor(part)
		if ok {
			m.partHeap.Push(cursor)
		}
	}

	if len(m.partHeap) == 0 {
		return m.result
	}

	// 初始化堆
	heap.Init(&(m.partHeap))
	curr := heap.Pop(&(m.partHeap)).(*PartCursor)
	for len(m.partHeap) >= 1 {
		// 出去下一个大于该分区的分区游标
		next := heap.Pop(&(m.partHeap)).(*PartCursor)

		// 取出小于等于下一个最大值的所有数据，合并到结果中
		part := curr.NextPartBeforeVal(next.currVal)
		for _, block := range part {
			m.result = append(m.result, block...)
		}

		// 如果没有取完则放回堆中
		if !curr.IsEnd() {
			heap.Push(&(m.partHeap), curr)
		}
		curr = next
	}

	// 剩余最后一个分区，取出剩余所有值
	part := curr.NextAll()
	for _, block := range part {
		m.result = append(m.result, block...)
	}

	return m.result
}

// ConcurrentMerger 并行合并器
type ConcurrentMerger struct {
	coSize int     // 协程数
	parts  []Part  // 需要合并的分区
}

func NewConcurrentMerger(size int) *ConcurrentMerger {
	return &ConcurrentMerger{
		coSize: size,
	}
}

func (m *ConcurrentMerger) AddPart(parts... Part) {
	m.parts = append(m.parts, parts...)
}

func (m *ConcurrentMerger) MergeIntoDataBlock() DataBlock {
	result := DataBlock{}
	// 预先分配好中间结果存储
	midResults := make([]DataBlock, m.coSize)

	cursors := make([]*PartCursor, len(m.parts))
	for i, part := range m.parts {
		cursors[i], _ = NewPartCursor(part)
	}

	// 将任务尽量平均分出不想交的分割点
	points := m.findSplitPoints()

	// 创建WaitGroup，等待拆分的任务都做完
	waitGroup := sync.WaitGroup{}
	for i := 0; i <= len(points); i++ {
		merger := BaseMerger{}
		for _, cursor := range cursors {
			// 取出每个分区分割点前的数据
			var part Part
			// 如果所有分隔点用完了，把最后的所有数据都取出来
			if i == len(points) {
				part = cursor.NextAll()
			} else {
				part = cursor.NextPartBeforeVal(points[i])
			}
			merger.AddPart(part)
		}
		waitGroup.Add(1)

		// 开启协程并发合并
		go func(num int) {
			// 按照索引独立赋值，不加锁
			midResults[num] = merger.MergeIntoDataBlock()
			waitGroup.Done()
		}(i)
	}
	waitGroup.Wait()

	// 因为每个分片不想交，直接追加，无需二次合并
	for _, midResult := range midResults {
		result = append(result, midResult...)
	}
	return result
}

// segment 数据分段信息，头尾数值和段内数量
// 用链表存储，便于进行分段的插入和合并
type segment struct {
	begin uint64    // 段起始数值
	end   uint64    // 段结尾数值
	size  int       // 段内包含的数据数量
	next  *segment  // 下一个段
}

// mergeNextSegment 合并后续相交段
// 将后续与当前段相交的段进行合并，跟新起始结尾点和数量
func mergeNextSegment(curr *segment) {
	next := curr.next

	for next != nil {
		if next.begin > curr.end + 1 {
			break
		}

		curr.size = curr.size + next.size
		curr.next = next.next
		if next.end >= curr.end {
			curr.end = next.end
			break
		}
		next = next.next
	}
}

// findSplitPoints 寻找分段点，把所有的分区按照并发数分成多个尽量数量均匀的组并发合并
func (m *ConcurrentMerger) findSplitPoints() (points []uint64) {
	var head *segment
	totalSize := 0

	for _, part := range m.parts {
		currSeg := head
		for _, block := range part {
			size := len(block)
			if size == 0 {
				continue
			}

			totalSize += size
			if head == nil {
				head = &segment{
					begin: block[0],
					end:   block[size - 1],
					size:  size,
				}
				currSeg = head
				continue
			}

			for currSeg != nil {
				if currSeg.next != nil &&
					block[0] >= currSeg.next.begin {
					// 同一个分区无相交情况，所以后续数据块起始点一定大于当前
					// 所以每个数据块无需从链表头部开始
					currSeg = currSeg.next
					continue
				}

				if block[0] <= currSeg.end + 1 {
					// 如果新的数据块起点落在当前段中，直接合并两段信息
					if currSeg.end < block[size - 1] {
						currSeg.end = block[size - 1]
					}
					currSeg.size = currSeg.size + size
				} else {
					// 如果起始点在当前段后，则插入一个新的段
					newSeg := &segment{
						begin: block[0],
						end:   block[size - 1],
						size:  size,
						next:  currSeg.next,
					}
					currSeg.next = newSeg
					currSeg = currSeg.next
				}

				// 段结尾可能被更新了，需要把当前段和后续相交的段进行合并
				mergeNextSegment(currSeg)
				break
			}
		}
	}

	// 根据总数量算出每个任务分片的需要的数据量大小
	splitSize := totalSize / m.coSize

	if head != nil {
		currSeg := head
		remainSize := splitSize
		currIdx := 0

		for len(points) < m.coSize - 1 {
			if currSeg == nil {
				break
			}

			if currSeg.size - currIdx <= remainSize {
				// 如果当前数量不足则继续往下找
				remainSize -= currSeg.size - currIdx
				if remainSize == 0 {
					// 数量刚好足够，以该段结尾为分隔点
					points = append(points, currSeg.end)
					remainSize = splitSize
				}
				currSeg = currSeg.next
				currIdx = 0
			} else {
				// 该段一部分能满足一个任务分片数量
				newIdx := currIdx + remainSize
				// 假设段内部相对均匀，从中按比例算出分割点
				point := uint64(
					math.Ceil(float64(currSeg.end - currSeg.begin) *
						(float64(newIdx) / float64(currSeg.size)))) + currSeg.begin
				points = append(points, point)
				currIdx = newIdx
				remainSize = splitSize
			}
		}
	}

	return points
}
