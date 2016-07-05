package lists

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	"github.com/getblank/blank-queue/common"
)

var (
	db     *bolt.DB
	lists  = map[string]*stat{}
	locker sync.Mutex
)

var (
	errOutOfRange  = errors.New("out of range")
	errListIsEmpty = errors.New("list is empty")
	errCurrupted   = errors.New("corrupted data")
)

type stat struct {
	Current      uint64   `json:"current"`
	Marked       []uint64 `json:"marked"`
	PrevSequence uint64   `json:"prevSequence"`
}

// Init is the main entrypoint for the package
func Init(file string) {
	var err error
	db, err = bolt.Open(file, 0644, nil)
	if err != nil {
		panic(err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			log.Info("Received an interrupt, need to close lists DB")
			db.Close()
			log.Info("App closed")
			close(signalChan)
			os.Exit(0)
		}
	}()
	log.Info("Lists DB started")
}

func Back(list string) (data interface{}, seq int, err error) {
	if Len(list) == 0 {
		return nil, 0, errListIsEmpty
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		c := elB.Cursor()
		k, v := c.Last()
		if k == nil {
			return errListIsEmpty
		}
		_seq := common.BytesToSeq(k)
		seq = int(_seq - common.ZeroPoint)
		err = json.Unmarshal(v, &data)
		if err != nil {
			return err
		}
		stats, err := getStat(list, b)
		if err != nil {
			return err
		}
		locker.Lock()
		defer locker.Unlock()
		current := stats.Current
		stats.Current = _seq
		err = saveStat(stats, b)
		if err != nil {
			stats.Current = current
		}
		return err
	})
	return data, seq, nil
}

func Drop(list string) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(list))
		if err != nil {
			return err
		}
		locker.Lock()
		defer locker.Unlock()
		delete(lists, list)
		return nil
	})
	return nil
}

func Front(list string) (data interface{}, seq int, err error) {
	if Len(list) == 0 {
		return nil, 0, errListIsEmpty
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		c := elB.Cursor()
		k, v := c.First()
		if k == nil {
			return errListIsEmpty
		}
		_seq := common.BytesToSeq(k)
		seq = int(_seq - common.ZeroPoint)
		err = json.Unmarshal(v, &data)
		if err != nil {
			return err
		}
		stats, err := getStat(list, b)
		if err != nil {
			return err
		}
		locker.Lock()
		defer locker.Unlock()
		current := stats.Current
		stats.Current = _seq
		err = saveStat(stats, b)
		if err != nil {
			stats.Current = current
		}
		return err
	})
	return data, seq, nil
}

func Len(list string) (l uint64) {
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		l = uint64(elB.Stats().KeyN)
		return nil
	})
	return l
}

func PushBack(list string, data interface{}) (n int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(list))
		if err != nil {
			return err
		}
		elB, err := b.CreateBucketIfNotExists(common.ElementsBucket)
		if err != nil {
			return err
		}
		var idBytes, seqBytes []byte
		if _id, ok := common.ExtractID(data); ok {
			idBytes = []byte(_id)
			_, err = common.GetEncodedSeqByID(list, idBytes, b)
			if err == nil {
				return common.ErrExists
			}
		}
		seq, err := nextShiftedSequence(elB)
		if err != nil {
			return err
		}
		seqBytes = common.SeqToBytes(seq)
		encoded, err := json.Marshal(data)
		if err != nil {
			return err
		}
		err = elB.Put(seqBytes, encoded)
		if err != nil {
			return err
		}
		if idBytes != nil {
			err = common.SetSeqToIDRef(seqBytes, idBytes, b)
			if err != nil {
				return err
			}
		}

		n = int(seq - common.ZeroPoint)
		return nil
	})
	return n, err
}

func PushFront(list string, data interface{}) (n int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(list))
		if err != nil {
			return err
		}
		elB, err := b.CreateBucketIfNotExists(common.ElementsBucket)
		if err != nil {
			return err
		}
		var idBytes, seqBytes []byte
		if _id, ok := common.ExtractID(data); ok {
			idBytes = []byte(_id)
			_, err = common.GetEncodedSeqByID(list, idBytes, b)
			if err == nil {
				return common.ErrExists
			}
		}
		seq, err := prevShiftedSequence(list, b)
		if err != nil {
			return err
		}
		seqBytes = common.SeqToBytes(seq)
		encoded, err := json.Marshal(data)
		if err != nil {
			return err
		}
		err = elB.Put(seqBytes, encoded)
		if err != nil {
			return err
		}
		if idBytes != nil {
			err = common.SetSeqToIDRef(seqBytes, idBytes, b)
			if err != nil {
				return err
			}
		}

		n = int(seq - common.ZeroPoint)
		return nil
	})
	return n, err
}

func Remove(list string, n int) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		seq := uint64(n) + common.ZeroPoint
		seqBytes := common.SeqToBytes(seq)
		err := elB.Delete(seqBytes)
		if err != nil {
			return err
		}
		sb := b.Bucket(common.SeqToIDBucket)
		if sb != nil {
			id := sb.Get(seqBytes)
			if id != nil {
				sb := b.Bucket(common.IDToSeqBucket)
				err = sb.Delete(id)
				if err != nil {
					return err
				}
			}
			err = sb.Delete(seqBytes)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func RemoveByID(list string, _id string) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		id := []byte(_id)
		seqBytes, err := common.GetEncodedSeqByID(list, id, b)
		if err != nil {
			return err
		}
		err = elB.Delete(seqBytes)
		if err != nil {
			return err
		}
		sb := b.Bucket(common.SeqToIDBucket)
		if sb != nil {
			err = sb.Delete(seqBytes)
			if err != nil {
				return err
			}
		}
		sb = b.Bucket(common.IDToSeqBucket)
		if sb != nil {
			err = sb.Delete(id)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func Next(list string) (data interface{}, seq int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		stats, err := getStat(list, b)
		if err != nil {
			return err
		}
		current := stats.Current
		seqBytes := common.SeqToBytes(current)
		c := elB.Cursor()
		k, _ := c.Seek(seqBytes)
		if k == nil {
			return common.ErrNotFound
		}
		if !bytes.Equal(seqBytes, k) {
			return errCurrupted
		}
		k, v := c.Next()
		if k == nil {
			return errOutOfRange
		}
		err = json.Unmarshal(v, &data)
		if err != nil {
			return err
		}
		stats.Current = common.BytesToSeq(k)
		err = saveStat(stats, b)
		if err != nil {
			stats.Current = current
		}
		seq = int(common.BytesToSeq(k) - common.ZeroPoint)
		return err
	})
	return data, seq, err
}

func Prev(list string) (data interface{}, seq int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(list))
		if b == nil {
			return common.ErrNotFound
		}
		elB := b.Bucket(common.ElementsBucket)
		if elB == nil {
			return common.ErrNotFound
		}
		stats, err := getStat(list, b)
		if err != nil {
			return err
		}
		current := stats.Current
		seqBytes := common.SeqToBytes(current)
		c := elB.Cursor()
		k, _ := c.Seek(seqBytes)
		if k == nil {
			return common.ErrNotFound
		}
		if !bytes.Equal(seqBytes, k) {
			return errCurrupted
		}
		k, v := c.Prev()
		if k == nil {
			return errOutOfRange
		}
		err = json.Unmarshal(v, &data)
		if err != nil {
			return err
		}
		stats.Current = common.BytesToSeq(k)
		err = saveStat(stats, b)
		if err != nil {
			stats.Current = current
		}
		seq = int(common.BytesToSeq(k) - common.ZeroPoint)
		return err
	})
	return data, seq, err
}

// nextShiftedSequence return next sequence for passed bucked shifted by ZeroPoint
func nextShiftedSequence(b *bolt.Bucket) (uint64, error) {
	s, err := b.NextSequence()
	if err != nil {
		return 0, err
	}
	return s + common.ZeroPoint, nil
}

func newStat() *stat {
	return &stat{0, []uint64{}, common.ZeroPoint}
}

// need to pass parent bucket
func prevShiftedSequence(list string, b *bolt.Bucket) (uint64, error) {
	stats, err := getStat(list, b)
	if err != nil {
		return 0, err
	}
	locker.Lock()
	defer locker.Unlock()
	seq := stats.PrevSequence
	stats.PrevSequence--
	err = saveStat(stats, b)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func getStat(list string, b *bolt.Bucket) (*stat, error) {
	locker.Lock()
	defer locker.Unlock()
	stats, ok := lists[list]
	if !ok {
		_stat := b.Get(common.StatBytes)
		if _stat == nil {
			stats = newStat()
			lists[list] = stats
			return stats, nil
		}
		err := json.Unmarshal(_stat, &stats)
		if err != nil {
			return nil, err
		}
	}
	return stats, nil
}

func saveStat(stats *stat, b *bolt.Bucket) error {
	encoded, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return b.Put(common.StatBytes, encoded)
}
