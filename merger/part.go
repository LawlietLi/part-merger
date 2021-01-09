package merger

// 数据块，连续的uint64数组
type DataBlock []uint64

// 分区，一个分区又多个无相交数据块构成
type Part []DataBlock

// PartCursor  分区游标，辅助读取分区
type PartCursor struct {
	Part      Part   // 读取的分区
	currBlock int    // 当前读取到的数据块编号
	currIdx   int    // 当前数据块的读取位置
	currVal   uint64 // 当前指向的值
}

// NewPartCursor 构造新的分区读取游标
// 讲游标放置到第一个有数的位置，成功则返回游标，如果整个数据分区为空则ok为false
func NewPartCursor(part Part) (new *PartCursor, ok bool) {
	new = &PartCursor{
		Part: part,
	}

	if len(new.Part) == 0 {
		return
	}

	for i, block := range new.Part {
		if len(block) > 0 {
			new.currBlock = i
			new.currVal  = block[0]
			ok = true
			return
		}
	}

	return
}

// Next 将游标指向分区的下一个位置
// 指向分区的下一个位置，如果已经到了最后则返回false
func (c *PartCursor) Next() (uint64, bool) {
	if c.currBlock >= len(c.Part) {
		return 0, false
	}

	if c.currIdx < len(c.Part[c.currBlock]) - 1 {
		c.currIdx++
		c.currVal = c.Part[c.currBlock][c.currIdx]
		return c.currVal, true
	}

	c.currBlock++
	if c.currBlock >= len(c.Part) {
		return 0, false
	}

	c.currIdx = 0
	for c.currBlock < len(c.Part) {
		if len(c.Part[c.currBlock]) > 0 {
			c.currVal = c.Part[c.currBlock][c.currIdx]
			return c.currVal, true
		}
		c.currBlock++
	}

	return 0, false
}

// NextPartBeforeVal 获取分区小于等于某个最大值的所有分片
// 传入需要取到的最大值，返回之前所有数据在Block上的切片，不做合并，减少底层数组内存拷贝
func (c *PartCursor) NextPartBeforeVal(maxVal uint64) (part Part) {
	if c.currVal > maxVal || c.IsEnd() {
		return part
	}

	block := c.Part[c.currBlock]
	for {
		if c.currBlock >= len(c.Part) {
			break
		}

		block = c.Part[c.currBlock]
		if block[len(block) - 1] <= maxVal {
			// 如果当前数据块的最后一个数小于maxVal，则整个分片剩余部分加入返回，并指向下一个数据块
			part = append(part, block[c.currIdx:])
			c.currIdx = len(block) - 1
			c.Next()
		} else {
			// 最后一个已经大于最大值，用二分查找找到第一个大于maxVal的位置，将其之前切片返回并退出循环
			l := c.currIdx
			r := len(block) - 1
			mid := 0

			for {
				mid = (l + r) / 2
				if block[mid] == maxVal {
					// 避免有重复值，继续往前检查
					for mid < len(block) - 1 {
						if block[mid + 1] == block[mid] {
							mid++
						} else {
							break
						}
					}
					mid = mid + 1
					break
				}

				if l >= r {
					if block[mid] <= maxVal {
						mid = mid + 1
					}
					break
				}

				if maxVal < block[mid] {
					r = mid - 1
				} else {
					l = mid + 1
				}
			}

			// 如果mid为0则说明该数据块的一个数就大于maxVal，过掉这个数据块
			if mid != 0 {
				part = append(part, block[c.currIdx:mid])
				c.currIdx = mid - 1
				c.Next()
			}

			break
		}
	}

	return part
}

// NextAll 获取余下所有的剩余数据块
func (c *PartCursor) NextAll() (part Part) {
	if c.IsEnd() {
		return
	}

	part = append(part, c.Part[c.currBlock][c.currIdx:])
	c.currIdx = 0
	c.currBlock++

	for c.currBlock < len(c.Part) {
		part = append(part, c.Part[c.currBlock])
		c.currBlock++
	}

	return
}

// Val 获取游标当前指向的值
func (c PartCursor) Val() uint64 {
	return c.currVal
}

// IsEnd 检查是否已经走到末尾
func (c *PartCursor) IsEnd() bool {
	return c.currBlock >= len(c.Part)
}

// PartCursorHeap 游标当前指向值构成的堆，辅助排序合并
type PartCursorHeap []*PartCursor

func (h PartCursorHeap) Len() int {
	return len(h)
}

func (h PartCursorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h PartCursorHeap) Less(i, j int) bool {
	return h[i].Val() < h[j].Val()
}

func (h *PartCursorHeap) Push(c interface{}) {
	*h = append(*h, c.(*PartCursor))
}

func (h *PartCursorHeap) Pop() (c interface{}) {
	n := len(*h)
	c = (*h)[n - 1]
	*h = (*h)[:n - 1]
	return
}