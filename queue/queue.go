package queue

import (
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
)

var (
	db                  *bolt.DB
	idToSeqBucket       = []byte("_id2seq")
	seqToIDBucket       = []byte("_seq2id")
	statBytes           = []byte("_stat")
	queues              = map[string]*queueStat{}
	queuesLocker        = new(sync.Mutex)
	errQueueIsNotExists = errors.New("queue is not exists")
	errSeqToIDBucket    = errors.New("seqToIDBucket is not exists")
	errNotFound         = errors.New("not found")
	errExistsInQ        = errors.New("item exists in queue")
)

type queueStat struct {
	Head    uint64   `json:"head"`
	Tail    uint64   `json:"tail"`
	Removed []uint64 `json:"removed"`
	sync.Mutex
}

// Drop drops queue and all it's items
func Drop(queue string) error {
	return drop(queue)
}

// Length returns queue length
func Length(queue string) uint64 {
	log.Debugf("Length request for queue: %s", queue)
	stat, err := getStat(queue, nil)
	if err != nil {
		return 0
	}
	stat.Lock()
	defer stat.Unlock()
	return stat.Tail - stat.Head - uint64(len(stat.Removed))
}

// Push adds item to queue
func Push(queue string, data interface{}) (err error) {
	log.Debugf("Push request to queue: %s", queue)
	return push(queue, data)
}

// Remove removes item from queue by provided string _id property
func Remove(queue string, _id string) error {
	log.Debugf("Remove request for queue: %s, _id: %s", queue, _id)
	return remove(queue, _id)
}

// Unshift returns item from queue with FIFO algorythm
func Unshift(queue string) (interface{}, error) {
	log.Debugf("Unshift request for queue: %s", queue)
	return unshift(queue)
}

// Init is a main entry point for package
func Init(file string) {
	var err error
	db, err = bolt.Open(file, 0600, nil)
	if err != nil {
		panic(err)
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			log.Info("Received an interrupt, need to close DB")
			db.Close()
			log.Info("App closed")
			close(signalChan)
			os.Exit(0)
		}
	}()
	log.Info("Queue DB started")
}

func bytesToSeq(b []byte) (seq uint64) {
	err := json.Unmarshal(b, &seq)
	if err != nil {
		log.Error(err)
	}
	return seq
}

func drop(queue string) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(queue))
		if b == nil {
			return errQueueIsNotExists
		}
		return tx.DeleteBucket([]byte(queue))
	})
	if err == nil {
		queuesLocker.Lock()
		defer queuesLocker.Unlock()
		delete(queues, queue)
	}
	return err
}

func extractID(data interface{}) (string, bool) {
	if m, ok := data.(map[string]interface{}); ok && m["_id"] != nil {
		if _id, ok := m["_id"].(string); ok && _id != "" {
			return _id, true
		}
	}
	return "", false
}

func getStat(queue string, b *bolt.Bucket) (*queueStat, error) {
	queuesLocker.Lock()
	defer queuesLocker.Unlock()
	stat := queues[queue]
	if stat != nil {
		return stat, nil
	}
	if b == nil {
		stat, err := getStatFromDb(queue)
		if err == nil {
			queues[queue] = stat
		}
		return stat, err
	}
	stat = &queueStat{Removed: []uint64{}}
	encoded := b.Get(statBytes)
	if encoded == nil {
		queues[queue] = stat
		return stat, nil
	}
	err := json.Unmarshal(encoded, stat)
	if err == nil {
		queues[queue] = stat
	}
	return stat, err
}

func getStatFromDb(queue string) (stat *queueStat, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(queue))
		if b == nil {
			stat = &queueStat{Removed: []uint64{}}
			return nil
		}
		encoded := b.Get(statBytes)
		err = json.Unmarshal(encoded, stat)
		if encoded == nil {
			stat = &queueStat{Removed: []uint64{}}
			return nil
		}
		return json.Unmarshal(encoded, stat)
	})
	return
}

func push(queue string, data interface{}) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		var seq uint64
		var encoded, id, seqBytes []byte
		var newQ, itemExists bool
		if b = tx.Bucket([]byte(queue)); b == nil {
			b, err = tx.CreateBucket([]byte(queue))
			if err != nil {
				return err
			}
			newQ = true
		}
		if _id, ok := extractID(data); ok {
			id = []byte(_id)
			if seqBytes, _ = getEncodedSeqByID(queue, id, b); seqBytes != nil {
				itemExists = true
			}
		}
		if seqBytes == nil {
			if !newQ {
				seq, err = b.NextSequence()
				if err != nil {
					return err
				}
			}
			seqBytes = seqToBytes(seq)
		}
		encoded, err = json.Marshal(data)
		if err != nil {
			return err
		}
		err = b.Put(seqBytes, encoded)
		if err != nil {
			return err
		}
		if !itemExists {
			if id != nil {
				err = setSeqToIDRef(seqBytes, id, b)
				if err != nil {
					return err
				}
			}
			err = setQueueTail(queue, seq+1, b)
		}
		return err
	})
	return err
}

func putStat(queue string, stat *queueStat, b *bolt.Bucket) error {
	encoded, err := json.Marshal(stat)
	if err != nil {
		return err
	}
	return b.Put(statBytes, encoded)
}

func remove(queue string, _id string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		b = tx.Bucket([]byte(queue))
		if b == nil {
			return errQueueIsNotExists
		}
		return removeByID(queue, []byte(_id), b)
	})
	return err
}

func getEncodedSeqByID(queue string, id []byte, b *bolt.Bucket) ([]byte, error) {
	sb := b.Bucket(idToSeqBucket)
	if sb == nil {
		return nil, errSeqToIDBucket
	}
	seqBytes := sb.Get(id)
	if seqBytes == nil {
		return nil, errNotFound
	}
	return seqBytes, nil
}

func removeByID(queue string, id []byte, b *bolt.Bucket) error {
	seqBytes, err := getEncodedSeqByID(queue, id, b)
	if err != nil {
		return err
	}
	sb := b.Bucket(idToSeqBucket)
	err = sb.Delete(id)
	if err != nil {
		return err
	}
	sb = b.Bucket(seqToIDBucket)
	if sb != nil {
		err = sb.Delete(seqBytes)
		if err != nil {
			return err
		}
	}
	err = b.Delete(seqBytes)
	if err == nil {
		seq := bytesToSeq(seqBytes)
		stat, err := getStat(queue, b)
		if err != nil {
			return err
		}
		stat.Lock()
		defer stat.Unlock()
		if stat.Removed == nil {
			stat.Removed = []uint64{}
		}
		stat.Removed = append(stat.Removed, seq)
	}
	return err
}

func removeRef(seq []byte, b *bolt.Bucket) error {
	sb := b.Bucket(seqToIDBucket)
	if sb == nil {
		return nil
	}
	id := sb.Get(seq)
	if id == nil {
		return nil
	}
	err := sb.Delete(seq)
	if err != nil {
		return err
	}
	sb = b.Bucket(idToSeqBucket)
	if sb == nil {
		return errSeqToIDBucket
	}
	return sb.Delete(id)
}

func setSeqToIDRef(seq, id []byte, b *bolt.Bucket) error {
	sb, err := b.CreateBucketIfNotExists(idToSeqBucket)
	if err != nil {
		return err
	}
	err = sb.Put(id, seq)
	if err != nil {
		return err
	}
	sb, err = b.CreateBucketIfNotExists(seqToIDBucket)
	if err != nil {
		return err
	}
	return sb.Put(seq, id)
}

func setQueueHead(queue string, head uint64, b *bolt.Bucket) error {
	stat, err := getStat(queue, b)
	if err != nil {
		return err
	}
	stat.Lock()
	defer stat.Unlock()
	stat.Head = head
	return putStat(queue, stat, b)
}

func setQueueTail(queue string, tail uint64, b *bolt.Bucket) error {
	stat, err := getStat(queue, b)
	if err != nil {
		return err
	}
	stat.Lock()
	defer stat.Unlock()
	stat.Tail = tail
	return putStat(queue, stat, b)
}

func seqToBytes(seq uint64) []byte {
	encoded, err := json.Marshal(seq)
	if err != nil {
		log.Error(err)
	}
	return encoded
}

func unshift(queue string) (data interface{}, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		var encoded []byte
		var stat *queueStat
		b = tx.Bucket([]byte(queue))
		if b == nil {
			return errQueueIsNotExists
		}
		stat, err = getStat(queue, b)
		if err != nil {
			return err
		}
		for seq := stat.Head; seq <= stat.Tail; seq++ {
			seqBytes := seqToBytes(seq)
			encoded = b.Get(seqBytes)
			if encoded == nil {
				continue
			}
			err = json.Unmarshal(encoded, &data)
			if err != nil {
				return err
			}
			err = removeRef(seqBytes, b)
			if err != nil {
				return err
			}
			setQueueHead(queue, seq+1, b)
			for i := len(stat.Removed) - 1; i >= 0; i-- {
				if stat.Removed[i] <= stat.Head {
					stat.Removed = append(stat.Removed[:i], stat.Removed[i+1:]...)
				}
			}
			return nil
		}
		return nil
	})
	return data, err
}
