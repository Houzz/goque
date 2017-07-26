package goque

import (
	"bytes"
	"encoding/gob"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
)

// Queue is a standard FIFO (first in, first out) queue.
type Queue struct {
	sync.RWMutex
	notEmpty chan bool
	DataDir  string
	db       *leveldb.DB
	head     uint64
	tail     uint64
	isOpen   bool
	closed   chan bool
}

// queue will wake up pop wait regularly based on FREQ_WAKE_UP_POP
// make this small to increase throughput, big to avoid busy check
const FREQ_WAKE_UP_POP = 100 * time.Millisecond

// OpenQueue opens a queue if one exists at the given directory. If one
// does not already exist, a new queue is created.
func OpenQueue(dataDir string) (*Queue, error) {
	var err error

	// Create a new Queue.
	q := &Queue{
		DataDir: dataDir,
		db:      &leveldb.DB{},
		head:    0,
		tail:    0,
		isOpen:  false,
	}
	q.notEmpty = make(chan bool, 1)

	// closed is used to signal pop waker to terminate
	q.closed = make(chan bool)

	// Open database for the queue.
	q.db, err = leveldb.OpenFile(dataDir, nil)
	if err != nil {
		return q, err
	}

	// Check if this Goque type can open the requested data directory.
	ok, err := checkGoqueType(dataDir, goqueQueue)
	if err != nil {
		return q, err
	}
	if !ok {
		return q, ErrIncompatibleType
	}

	// Set isOpen and return.
	q.isOpen = true
	return q, q.init()
}

// Enqueue adds an item to the queue.
func (q *Queue) Enqueue(value []byte) (*Item, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Create new Item.
	item := &Item{
		ID:    q.tail + 1,
		Key:   idToKey(q.tail + 1),
		Value: value,
	}

	// Add it to the queue.
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	// Increment tail position.
	q.tail++

	// This is the best effort in terms of signaling
	select {
	case q.notEmpty <- true:
	default:
	}

	return item, nil
}

// EnqueueString is a helper function for Enqueue that accepts a
// value as a string rather than a byte slice.
func (q *Queue) EnqueueString(value string) (*Item, error) {
	return q.Enqueue([]byte(value))
}

// EnqueueObject is a helper function for Enqueue that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
func (q *Queue) EnqueueObject(value interface{}) (*Item, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return q.Enqueue(buffer.Bytes())
}

/* Pop removes and returns an item from the queue.

If optional args 'block' is true and 'timeout' is not given, block if necessary
until an item is available.
If 'timeout' is a non-negative time duration, it blocks at most 'timeout'
millisecond and raises the Empty exception if no item was available within that
time.
Otherwise ('block' is false), return an item if one is immediately available,
else raise the error we caught ('timeout' is ignored in that case).
*/
func (q *Queue) Pop(block bool, timeout ...time.Duration) (*Item, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	if !block {
		if q.Length() == 0 {
			return nil, ErrEmpty
		}
	} else if timeout == nil {
		for q.Length() == 0 {
			q.Unlock()
			<-q.notEmpty
			q.Lock()
		}
	} else {
		if len(timeout) > 1 {
			glog.V(1).Info("only the first timeout accepted, others ignored")
		}

		start := time.Now()
		timer := time.NewTimer(timeout[0])
		for q.Length() == 0 {
			q.Unlock()
			select {
			case <-q.notEmpty:
			case <-timer.C:
			}
			q.Lock()

			remaining := timeout[0] - time.Now().Sub(start)
			if remaining <= 0 {
				break
			}
			timer.Reset(remaining)
		}
		timer.Stop()
	}

	// Try to get the next item in the queue.
	item, err := q.getItemByID(q.head + 1)
	if err != nil {
		return nil, err
	}

	// Remove this item from the queue.
	if err := q.db.Delete(item.Key, nil); err != nil {
		return nil, err
	}

	// Increment head position.
	q.head++

	return item, nil
}

// Dequeue removes and returns the next item in the queue without blocking
func (q *Queue) Dequeue() (*Item, error) {
	if item, err := q.Pop(false); err != nil {
		return nil, err
	} else {
		return item, nil
	}
}

// Peek returns the next item in the queue without removing it.
func (q *Queue) Peek() (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(q.head + 1)
}

// PeekByOffset returns the item located at the given offset,
// starting from the head of the queue, without removing it.
func (q *Queue) PeekByOffset(offset uint64) (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(q.head + offset + 1)
}

// PeekByID returns the item with the given ID without removing it.
func (q *Queue) PeekByID(id uint64) (*Item, error) {
	q.RLock()
	defer q.RUnlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	return q.getItemByID(id)
}

// Update updates an item in the queue without changing its position.
func (q *Queue) Update(id uint64, newValue []byte) (*Item, error) {
	q.Lock()
	defer q.Unlock()

	// Check if queue is closed.
	if !q.isOpen {
		return nil, ErrDBClosed
	}

	// Check if item exists in queue.
	if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}

	// Create new Item.
	item := &Item{
		ID:    id,
		Key:   idToKey(id),
		Value: newValue,
	}

	// Update this item in the queue.
	if err := q.db.Put(item.Key, item.Value, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// UpdateString is a helper function for Update that accepts a value
// as a string rather than a byte slice.
func (q *Queue) UpdateString(id uint64, newValue string) (*Item, error) {
	return q.Update(id, []byte(newValue))
}

// UpdateObject is a helper function for Update that accepts any
// value type, which is then encoded into a byte slice using
// encoding/gob.
func (q *Queue) UpdateObject(id uint64, newValue interface{}) (*Item, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(newValue); err != nil {
		return nil, err
	}
	return q.Update(id, buffer.Bytes())
}

// Length returns the total number of items in the queue.
func (q *Queue) Length() uint64 {
	return q.tail - q.head
}

// Close closes the LevelDB database of the queue.
func (q *Queue) Close() error {
	q.Lock()
	defer q.Unlock()

	// Check if queue is already closed.
	if !q.isOpen {
		return nil
	}

	// Close the LevelDB database.
	if err := q.db.Close(); err != nil {
		return err
	}

	// Reset queue head and tail,
	// set isOpen to false,
	// and signal pop waker to terminate.
	q.head = 0
	q.tail = 0
	q.isOpen = false
	close(q.closed)

	return nil
}

// Drop closes and deletes the LevelDB database of the queue.
func (q *Queue) Drop() error {
	if err := q.Close(); err != nil {
		return err
	}

	return os.RemoveAll(q.DataDir)
}

// getItemByID returns an item, if found, for the given ID.
func (q *Queue) getItemByID(id uint64) (*Item, error) {
	// Check if empty or out of bounds.
	if q.Length() == 0 {
		return nil, ErrEmpty
	} else if id <= q.head || id > q.tail {
		return nil, ErrOutOfBounds
	}

	// Get item from database.
	var err error
	item := &Item{ID: id, Key: idToKey(id)}
	if item.Value, err = q.db.Get(item.Key, nil); err != nil {
		return nil, err
	}

	return item, nil
}

// init initializes the queue data.
func (q *Queue) init() error {
	// Create a new LevelDB Iterator.
	iter := q.db.NewIterator(nil, nil)
	defer iter.Release()

	// Set queue head to the first item.
	if iter.First() {
		q.head = keyToID(iter.Key()) - 1
	}

	// Set queue tail to the last item.
	if iter.Last() {
		q.tail = keyToID(iter.Key())
	}

	// regularly wake up pop to avoid infinite block, return when queue is closed
	go func(q *Queue) {
		for {
			select {
			case <-time.After(FREQ_WAKE_UP_POP):
				select {
				case q.notEmpty <- true:
				default:
				}
			case <-q.closed:
				return
			}
		}
	}(q)

	return iter.Error()
}
