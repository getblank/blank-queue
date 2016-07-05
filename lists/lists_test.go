package lists

import (
	"os"
	"testing"

	. "github.com/franela/goblin"
	"github.com/getblank/blank-queue/common"
)

var fileName = "lists-test.db"

func Test(t *testing.T) {
	g := Goblin(t)
	os.Remove(fileName)
	Init(fileName)

	g.Describe("#PushBack", func() {
		g.It("should put element to the back of the list and return its sequence number", func() {
			list := "PushBackListTest"
			n, err := PushBack(list, "testData")
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(1)

			n, err = PushBack(list, "testData2")
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(2)

			e, n, err := Back(list)
			g.Assert(n).Equal(2)
			g.Assert(e).Equal("testData2")
		})
	})

	g.Describe("#PushFront", func() {
		g.It("should put element to the front of the list and return its sequence number", func() {
			list := "PushFrontListTest"
			n, err := PushFront(list, "testData")
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(0)

			n, err = PushFront(list, "testData2")
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(-1)

			e, n, err := Front(list)
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(-1)
			g.Assert(e).Equal("testData2")
		})
	})

	g.Describe("#Front", func() {
		g.It("should move cursor to the first element the list, return element and its sequence number", func() {
			list := "FrontListTest"
			n, _ := PushBack(list, "testData")

			n, _ = PushBack(list, "testData2")

			e, n, err := Front(list)
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(1)
			g.Assert(e).Equal("testData")

			stats, ok := lists[list]
			g.Assert(ok).IsTrue()
			g.Assert(stats.Current - common.ZeroPoint).Equal(uint64(1))
		})
	})

	g.Describe("#Back", func() {
		g.It("should move cursor to the last element the list, return element and its sequence number", func() {
			list := "BackListTest"
			n, _ := PushBack(list, "testData")

			n, _ = PushBack(list, "testData2")

			e, n, err := Back(list)
			g.Assert(err == nil).IsTrue()
			g.Assert(n).Equal(2)
			g.Assert(e).Equal("testData2")

			stats, ok := lists[list]
			g.Assert(ok).IsTrue()
			g.Assert(stats.Current - common.ZeroPoint).Equal(uint64(2))
		})
	})

	g.Describe("#Next", func() {
		g.It("should move cursor to the next element the list, return element and its sequence number", func() {
			list := "NextListTest"
			n, _ := PushBack(list, "testData1")
			n, _ = PushBack(list, "testData2")
			n, _ = PushBack(list, "testData3")

			Front(list)

			e, n, err := Next(list)
			g.Assert(e).Equal("testData2")
			g.Assert(n).Equal(2)
			g.Assert(err == nil).IsTrue()
			stats, ok := lists[list]
			g.Assert(ok).IsTrue()
			g.Assert(stats.Current - common.ZeroPoint).Equal(uint64(2))

			e, n, err = Next(list)
			g.Assert(e).Equal("testData3")
			g.Assert(n).Equal(3)
			g.Assert(err == nil).IsTrue()
			g.Assert(stats.Current - common.ZeroPoint).Equal(uint64(3))
		})

		g.It("should return errOutOfRange when no prev element", func() {
			list := "NextErrListTest"
			PushBack(list, "testData1")
			PushBack(list, "testData2")
			PushBack(list, "testData3")

			Back(list)
			_, _, err := Next(list)
			g.Assert(err).Equal(errOutOfRange)
		})

	})

	g.Describe("#Prev", func() {
		g.It("should move cursor to the previous element the list, return element and its sequence number", func() {
			list := "PrevListTest"
			n, _ := PushBack(list, "testData1")
			n, _ = PushBack(list, "testData2")
			n, _ = PushBack(list, "testData3")

			Back(list)

			e, n, err := Prev(list)
			g.Assert(e).Equal("testData2")
			g.Assert(n).Equal(2)
			g.Assert(err == nil).IsTrue()
			stats, ok := lists[list]
			g.Assert(ok).IsTrue()
			g.Assert(stats.Current - common.ZeroPoint).Equal(uint64(2))

			e, n, err = Prev(list)
			g.Assert(e).Equal("testData1")
			g.Assert(n).Equal(1)
			g.Assert(err == nil).IsTrue()
			g.Assert(stats.Current - common.ZeroPoint).Equal(uint64(1))
		})

		g.It("should return errOutOfRange when no prev element", func() {
			list := "PrevErrListTest"
			PushBack(list, "testData1")
			PushBack(list, "testData2")
			PushBack(list, "testData3")

			Front(list)
			_, _, err := Prev(list)
			g.Assert(err).Equal(errOutOfRange)
		})
	})

	g.Describe("#Remove", func() {
		g.It("should remove element from the list by sequence number passed", func() {
			list := "RemoveListTest"
			PushBack(list, "testData1")
			PushBack(list, "testData2")
			PushBack(list, "testData3")

			Front(list)
			err := Remove(list, 2)
			g.Assert(err == nil).IsTrue()
			e, n, err := Next(list)
			g.Assert(e).Equal("testData3")
			g.Assert(n).Equal(3)
			g.Assert(err == nil).IsTrue()
		})
	})

	g.Describe("#Len", func() {
		g.It("should return zero if list is empty or new", func() {
			l := Len("zeroLenTest")
			g.Assert(l).Equal(uint64(0))
		})
	})

	g.Describe("#RemoveByID", func() {
		g.It("should remove element from the list by _id property passed", func() {
			list := "RemoveByIDListTest"
			PushBack(list, map[string]interface{}{"_id": "1", "data": "testData1"})
			PushBack(list, map[string]interface{}{"_id": "2", "data": "testData2"})
			PushBack(list, map[string]interface{}{"_id": "3", "data": "testData3"})

			err := RemoveByID(list, "1")
			g.Assert(err == nil).IsTrue()
			e, n, err := Front(list)
			g.Assert(e).Equal(map[string]interface{}{"_id": "2", "data": "testData2"})
			g.Assert(n).Equal(2)
			g.Assert(err == nil).IsTrue()
		})
	})

	g.Describe("#Drop", func() {
		g.It("should totally drop list", func() {
			list := "DropListTest"
			PushBack(list, map[string]interface{}{"_id": "1", "data": "testData1"})
			PushBack(list, map[string]interface{}{"_id": "2", "data": "testData3"})
			PushBack(list, map[string]interface{}{"_id": "3", "data": "testData4"})

			err := Drop(list)
			g.Assert(err == nil).IsTrue()
			_, _, err = Front(list)
			g.Assert(err).Equal(errListIsEmpty)
		})
	})

	os.Remove(fileName)
}
