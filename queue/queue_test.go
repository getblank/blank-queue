package queue

import (
	"os"
	"testing"

	. "github.com/franela/goblin"
)

var fileName = "queue.db"

var strings = []string{"0", "1", "2", "3", "4", "5"}

var maps = []map[string]interface{}{
	{"_id": "0", "data": "00"},
	{"_id": "1", "data": "11"},
	{"_id": "2", "data": "22"},
	{"_id": "3", "data": "33"},
	{"_id": "4", "data": "44"},
}

func Test(t *testing.T) {
	g := Goblin(t)
	os.Remove(fileName)
	Init(fileName)
	g.Describe("#Push", func() {
		g.It("should create queue and statistic", func() {
			queue := "test1"
			err := Push(queue, 1)
			g.Assert(err == nil).IsTrue()
			g.Assert(queues[queue] != nil).IsTrue()
		})
		g.It("should add item to the queue and increase it length", func() {
			queue := "test2"
			err := Push(queue, 2)
			g.Assert(err == nil).IsTrue()
			g.Assert(int(Length(queue))).Equal(1)
		})
		g.It("should replace queued item when pushed item with id existed in the queue", func() {
			queue := "test22"
			err := Push(queue, map[string]interface{}{"_id": "1", "data": 1})
			g.Assert(err == nil).IsTrue()
			g.Assert(int(Length(queue))).Equal(1)
			err = Push(queue, map[string]interface{}{"_id": "2", "data": 3})
			g.Assert(err == nil).IsTrue()
			g.Assert(int(Length(queue))).Equal(2)
			err = Push(queue, map[string]interface{}{"_id": "1", "data": 4})
			g.Assert(err == nil).IsTrue()
			g.Assert(int(Length(queue))).Equal(2)
			item, err := Unshift(queue)
			g.Assert(err == nil).IsTrue()
			g.Assert(int(Length(queue))).Equal(1)
			g.Assert(item.(map[string]interface{})["data"].(float64)).Equal(float64(4))
		})
	})

	g.Describe("#Unshift", func() {
		g.It("should unshift items from queue in FIFO order", func() {
			queue := "test3"
			for _, p := range strings {
				err := Push(queue, p)
				g.Assert(err == nil).IsTrue()
			}
			g.Assert(int(Length(queue))).Equal(6)
			for _, p := range strings {
				item, err := Unshift(queue)
				g.Assert(err == nil).IsTrue()
				g.Assert(item.(string)).Equal(p)
			}
			g.Assert(int(Length(queue))).Equal(0)
		})
		g.It("should return error when queue is not exists", func() {
			queue := "testErrUnshift"
			_, err := Unshift(queue)
			g.Assert(err).Equal(errQueueIsNotExists)
		})

	})

	g.Describe("#Remove", func() {
		g.It("should remove item from queue", func() {
			queue := "test4"
			for _, p := range maps {
				err := Push(queue, p)
				g.Assert(err == nil).IsTrue()
			}
			g.Assert(int(Length(queue))).Equal(5)
			err := Remove(queue, "2")
			g.Assert(int(Length(queue))).Equal(4)
			g.Assert(err == nil).IsTrue()
			for i, p := range maps {
				if i == 2 {
					continue
				}
				item, err := Unshift(queue)
				g.Assert(err == nil).IsTrue()
				g.Assert(item.(map[string]interface{})).Equal(p)
			}
			g.Assert(int(Length(queue))).Equal(0)
		})
		g.It("should return error when queue is not exists", func() {
			queue := "test5"
			err := Remove(queue, "1")
			g.Assert(err).Equal(errQueueIsNotExists)
		})
	})

	g.Describe("#Length", func() {
		g.It("should return zero when queue is not exists", func() {
			queue := "test6"
			g.Assert(Length(queue)).Equal(uint64(0))
		})
	})

	g.Describe("#Drop", func() {
		g.It("should drop queue and all it's items", func() {
			queue := "testDrop"
			g.Assert(Length(queue)).Equal(uint64(0))
			for _, p := range strings {
				err := Push(queue, p)
				g.Assert(err == nil).IsTrue()
			}
			g.Assert(Length(queue)).Equal(uint64(6))
			err := Drop(queue)
			g.Assert(err == nil).IsTrue()
			err = Drop(queue)
			g.Assert(err == nil).IsFalse("queue must be already dropped")
			_, err = Unshift(queue)
			g.Assert(err == nil).IsFalse("queue must be already dropped")
			g.Assert(Length(queue)).Equal(uint64(0))
		})
	})

	g.Describe("#Get", func() {
		g.It("should return item from queue by provided _id, but keep it in queue untoched", func() {
			queue := "testGet"
			g.Assert(Length(queue)).Equal(uint64(0))
			for _, p := range maps {
				err := Push(queue, p)
				g.Assert(err == nil).IsTrue()
			}
			g.Assert(Length(queue)).Equal(uint64(5))
			_item, err := Get(queue, "1")
			g.Assert(err == nil).IsTrue()
			g.Assert(Length(queue)).Equal(uint64(5))
			item := _item.(map[string]interface{})
			g.Assert(item["_id"].(string)).Equal("1")
			g.Assert(item["data"].(string)).Equal("11")
		})
	})
}
