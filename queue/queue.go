package queue

import (
	"bytes"
	"encoding/binary"
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
)

type queueStat struct {
	Head uint64 `json:"head"`
	Tail uint64 `json:"tail"`
	sync.Mutex
}

// Push adds item to queue
func Push(queue string, data interface{}) (err error) {
	return push(queue, data)
}

// Unshift returns item from queue with FIFO algorythm
func Unshift(queue string) (interface{}, error) {
	return unshift(queue)
}

// Remove removes item from queue by provided string _id property
func Remove(queue string, _id string) error {
	return remove(queue, _id)
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
	stat = new(queueStat)
	encoded := b.Get(statBytes)
	if encoded == nil {
		return stat, nil
	}
	err := json.Unmarshal(encoded, stat)
	if err == nil {
		queues[queue] = stat
	}
	return stat, err
}
func push(queue string, data interface{}) (err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		var seq uint64
		var encoded []byte
		b, err = tx.CreateBucketIfNotExists([]byte(queue))
		if err != nil {
			return err
		}
		seq, err = b.NextSequence()
		if err != nil {
			return err
		}
		encoded, err = json.Marshal(data)
		if err != nil {
			return err
		}
		seqBytes := seqToBytes(seq)
		err = b.Put(seqBytes, encoded)
		if err != nil {
			return err
		}
		if _id, ok := extractID(data); ok {
			id := []byte(_id)
			err = setSeqToIDRef(seqBytes, id, b)
			if err != nil {
				return err
			}
		}
		err = setQueueTail(queue, seq, b)
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
		return removeByID([]byte(_id), b)
	})
	return err
}

func removeByID(id []byte, b *bolt.Bucket) error {
	sb := b.Bucket(idToSeqBucket)
	if sb == nil {
		return errSeqToIDBucket
	}
	seq := sb.Get(id)
	if seq == nil {
		return errNotFound
	}
	err := sb.Delete(id)
	if err != nil {
		return err
	}
	sb = b.Bucket(seqToIDBucket)
	if sb != nil {
		err = sb.Delete(seq)
		if err != nil {
			return err
		}
	}
	return b.Delete(seq)
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
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, seq)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
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
			stat.Head = seq + 1
			return nil
		}
		return nil
	})
	return data, err
}
