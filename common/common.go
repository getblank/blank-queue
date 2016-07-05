package common

import (
	"errors"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
)

// Constanst to correctly converts uint64 sequences to int
const (
	MaxUint   = ^uint64(0)
	ZeroPoint = MaxUint / 1000000000
)

// common sub buckets names
var (
	ElementsBucket = []byte("_elements")
	IDToSeqBucket  = []byte("_id2seq")
	SeqToIDBucket  = []byte("_seq2id")
	StatBytes      = []byte("_stat")
)

// common errors
var (
	ErrNotFound         = errors.New("not found")
	ErrSeqToIDBucket    = errors.New("seqToIDBucket is not exists")
	ErrExists           = errors.New("element exists")
	ErrNoIDInTheElement = errors.New("no id in element")
)

// BytesToSeq converts []byte implementation of sequence to uint64
func BytesToSeq(b []byte) (seq uint64) {
	seq, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		log.Error(err)
	}
	return seq
}

// ExtractID returns _id property of the passed interface{} if it is a map[string]interface{}
func ExtractID(data interface{}) (string, bool) {
	if m, ok := data.(map[string]interface{}); ok && m["_id"] != nil {
		if _id, ok := m["_id"].(string); ok && _id != "" {
			return _id, true
		}
	}
	return "", false
}

// GetEncodedSeqByID returns []byte representation of the item sequence key by item _id property
func GetEncodedSeqByID(queue string, id []byte, b *bolt.Bucket) ([]byte, error) {
	sb := b.Bucket(IDToSeqBucket)
	if sb == nil {
		return nil, ErrSeqToIDBucket
	}
	seqBytes := sb.Get(id)
	if seqBytes == nil {
		return nil, ErrNotFound
	}
	return seqBytes, nil
}

// IntToUint adds ZeroPoint offset to int input argument and convert value to uint64
func IntToUint(i int) uint64 {
	return ZeroPoint + uint64(i)
}

// SetSeqToIDRef creates index records when key is a sequence and value is a item _id
func SetSeqToIDRef(seq, id []byte, b *bolt.Bucket) error {
	sb, err := b.CreateBucketIfNotExists(IDToSeqBucket)
	if err != nil {
		return err
	}
	err = sb.Put(id, seq)
	if err != nil {
		return err
	}
	sb, err = b.CreateBucketIfNotExists(SeqToIDBucket)
	if err != nil {
		return err
	}
	return sb.Put(seq, id)
}

// SeqToBytes converts uint64 implementation of sequence to []byte
func SeqToBytes(seq uint64) []byte {
	str := strconv.FormatUint(seq, 10)
	return []byte(str)
}

// UintToInt subtract ZeroPoint offset from uint64 input argument and convert value to int
func UintToInt(u uint64) int {
	return int(ZeroPoint - u)
}
