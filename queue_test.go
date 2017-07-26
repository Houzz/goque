package goque

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestQueueClose(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	if _, err := q.EnqueueString("value"); err != nil {
		t.Error(err)
	}

	if q.Length() != 1 {
		t.Errorf("Expected queue length of 1, got %d", q.Length())
	}

	q.Close()

	if _, err := q.Dequeue(); err != ErrDBClosed {
		t.Errorf("Expected to get database closed error, got %s", err.Error())
	}

	if q.Length() != 0 {
		t.Errorf("Expected queue length of 0, got %d", q.Length())
	}
}

func TestQueueDrop(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}

	if _, err = os.Stat(file); os.IsNotExist(err) {
		t.Error(err)
	}

	q.Drop()

	if _, err = os.Stat(file); err == nil {
		t.Error("Expected directory for test database to have been deleted")
	}
}

func TestQueueIncompatibleType(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pq, err := OpenPriorityQueue(file, ASC)
	if err != nil {
		t.Error(err)
	}
	defer pq.Drop()
	pq.Close()

	if _, err = OpenQueue(file); err != ErrIncompatibleType {
		t.Error("Expected priority queue to return ErrIncompatibleTypes when opening goquePriorityQueue")
	}
}

func TestQueueEnqueue(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue size of 10, got %d", q.Length())
	}
}

func TestQueueDequeue(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue length of 10, got %d", q.Length())
	}

	deqItem, err := q.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if q.Length() != 9 {
		t.Errorf("Expected queue length of 9, got %d", q.Length())
	}

	compStr := "value for item 1"

	if deqItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, deqItem.ToString())
	}
}

func TestQueuePeek(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	compStr := "value for item"

	if _, err := q.EnqueueString(compStr); err != nil {
		t.Error(err)
	}

	peekItem, err := q.Peek()
	if err != nil {
		t.Error(err)
	}

	if peekItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, peekItem.ToString())
	}

	if q.Length() != 1 {
		t.Errorf("Expected queue length of 1, got %d", q.Length())
	}
}

func TestQueuePeekByOffset(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	compStrFirst := "value for item 1"
	compStrLast := "value for item 10"
	compStr := "value for item 4"

	peekFirstItem, err := q.PeekByOffset(0)
	if err != nil {
		t.Error(err)
	}

	if peekFirstItem.ToString() != compStrFirst {
		t.Errorf("Expected string to be '%s', got '%s'", compStrFirst, peekFirstItem.ToString())
	}

	peekLastItem, err := q.PeekByOffset(9)
	if err != nil {
		t.Error(err)
	}

	if peekLastItem.ToString() != compStrLast {
		t.Errorf("Expected string to be '%s', got '%s'", compStrLast, peekLastItem.ToString())
	}

	peekItem, err := q.PeekByOffset(3)
	if err != nil {
		t.Error(err)
	}

	if peekItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, peekItem.ToString())
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue length of 10, got %d", q.Length())
	}
}

func TestQueuePeekByID(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	compStr := "value for item 3"

	peekItem, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	if peekItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, peekItem.ToString())
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue length of 10, got %d", q.Length())
	}
}

func TestQueueUpdate(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	item, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	oldCompStr := "value for item 3"
	newCompStr := "new value for item 3"

	if item.ToString() != oldCompStr {
		t.Errorf("Expected string to be '%s', got '%s'", oldCompStr, item.ToString())
	}

	updatedItem, err := q.Update(item.ID, []byte(newCompStr))
	if err != nil {
		t.Error(err)
	}

	if updatedItem.ToString() != newCompStr {
		t.Errorf("Expected current item value to be '%s', got '%s'", newCompStr, item.ToString())
	}

	newItem, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	if newItem.ToString() != newCompStr {
		t.Errorf("Expected new item value to be '%s', got '%s'", newCompStr, item.ToString())
	}
}

func TestQueueUpdateString(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	item, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	oldCompStr := "value for item 3"
	newCompStr := "new value for item 3"

	if item.ToString() != oldCompStr {
		t.Errorf("Expected string to be '%s', got '%s'", oldCompStr, item.ToString())
	}

	updatedItem, err := q.UpdateString(item.ID, newCompStr)
	if err != nil {
		t.Error(err)
	}

	if updatedItem.ToString() != newCompStr {
		t.Errorf("Expected current item value to be '%s', got '%s'", newCompStr, item.ToString())
	}

	newItem, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	if newItem.ToString() != newCompStr {
		t.Errorf("Expected new item value to be '%s', got '%s'", newCompStr, item.ToString())
	}
}

func TestQueueUpdateObject(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	type object struct {
		Value int
	}

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueObject(object{i}); err != nil {
			t.Error(err)
		}
	}

	item, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	oldCompObj := object{3}
	newCompObj := object{33}

	var obj object
	if err := item.ToObject(&obj); err != nil {
		t.Error(err)
	}

	if obj != oldCompObj {
		t.Errorf("Expected object to be '%+v', got '%+v'", oldCompObj, obj)
	}

	updatedItem, err := q.UpdateObject(item.ID, newCompObj)
	if err != nil {
		t.Error(err)
	}

	if err := updatedItem.ToObject(&obj); err != nil {
		t.Error(err)
	}

	if obj != newCompObj {
		t.Errorf("Expected current object to be '%+v', got '%+v'", newCompObj, obj)
	}

	newItem, err := q.PeekByID(3)
	if err != nil {
		t.Error(err)
	}

	if err := newItem.ToObject(&obj); err != nil {
		t.Error(err)
	}

	if obj != newCompObj {
		t.Errorf("Expected new object to be '%+v', got '%+v'", newCompObj, obj)
	}
}

func TestQueueUpdateOutOfBounds(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 1; i <= 10; i++ {
		if _, err := q.EnqueueString(fmt.Sprintf("value for item %d", i)); err != nil {
			t.Error(err)
		}
	}

	if q.Length() != 10 {
		t.Errorf("Expected queue length of 10, got %d", q.Length())
	}

	deqItem, err := q.Dequeue()
	if err != nil {
		t.Error(err)
	}

	if q.Length() != 9 {
		t.Errorf("Expected queue length of 9, got %d", q.Length())
	}

	if _, err = q.Update(deqItem.ID, []byte(`new value`)); err != ErrOutOfBounds {
		t.Errorf("Expected to get queue out of bounds error, got %s", err.Error())
	}

	if _, err = q.Update(deqItem.ID+1, []byte(`new value`)); err != nil {
		t.Error(err)
	}
}

func TestQueueEmpty(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	_, err := q.EnqueueString("value for item")
	if err != nil {
		t.Error(err)
	}

	_, err = q.Dequeue()
	if err != nil {
		t.Error(err)
	}

	_, err = q.Dequeue()
	if err != ErrEmpty {
		t.Errorf("Expected to get empty error, got %s", err.Error())
	}
}

func TestQueueOutOfBounds(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	_, err := q.EnqueueString("value for item")
	if err != nil {
		t.Error(err)
	}

	_, err = q.PeekByOffset(2)
	if err != ErrOutOfBounds {
		t.Errorf("Expected to get queue out of bounds error, got %s", err.Error())
	}
}

func BenchmarkQueueEnqueue(b *testing.B) {
	// Open test database
	q, teardown := setupBenchmark(b)
	defer teardown()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = q.Enqueue([]byte("value"))
	}
}

func BenchmarkQueueDequeue(b *testing.B) {
	// Open test database
	q, teardown := setupBenchmark(b)
	defer teardown()
	var err error

	// Fill with dummy data
	for n := 0; n < b.N; n++ {
		if _, err = q.Enqueue([]byte("value")); err != nil {
			b.Error(err)
		}
	}

	// Start benchmark
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = q.Dequeue()
	}
}

/* Tests added by tai to test blocking feature */

func TestQueueEnqueueObject(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 0; i < 10; i++ {
		if _, err := q.EnqueueObject(i); err != nil {
			t.Fatalf("got error when pushing items into queue: %s", err)
		}
	}

	for i := 0; i < 10; i++ {
		if _, err := q.EnqueueObject(fmt.Sprintf("string item %d", i)); err != nil {
			t.Fatalf("got error when pushing items into queue: %s", err)
		}
	}

	// check all items have been pushed to queue
	if l := q.Length(); l != 20 {
		t.Fatalf("queue length does not match expectation, expect 20, got %s", l)
	}
}

func TestQueuePop(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	for i := 0; i < 10; i++ {
		if _, err := q.EnqueueObject(fmt.Sprintf("string item %d", i)); err != nil {
			t.Fatalf("got error when pushing items into queue: %s", err)
		}
	}

	if l := q.Length(); l != 10 {
		t.Fatalf("queue length does not match expectation, expect 10, got %s", l)
	}

	for i := 0; i < 10; i++ {
		item, err := q.Pop(false)
		if err != nil {
			t.Fatalf("got error when poping items from queue: %s", err)
		}
		expected := fmt.Sprintf("string item %d", i)
		var actual string
		item.ToObject(&actual)
		if expected != actual {
			t.Fatalf("poped item doesn't match expectation, expect %s, got %s", expected, actual)
		}
	}

	if q.Length() != 0 {
		t.Fatalf("queue should be empty but still has %d items", q.Length())
	}

	if item, err := q.Pop(false); item != nil || err != ErrEmpty {
		t.Fatalf("expected empty error but got item %s, and error %s", item, err)
	}
}

func TestQueuePopNoWait(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	// use channel to make sure test is done
	msg := make(chan string, 1)

	go func() {
		if item, err := q.Dequeue(); item != nil {
			errMsg := fmt.Sprintf("shouldn't pop out any item but got: %s", item)
			msg <- errMsg
		} else if err != ErrEmpty {
			errMsg := fmt.Sprintf("got error when poping from queue: %s", err)
			msg <- errMsg
		} else {
			msg <- ""
		}
	}()

	// wait a while to let PopNoWait return
	time.Sleep(100 * time.Millisecond)
	q.EnqueueObject("string item")
	if errMsg, ok := <-msg; !ok {
		t.Fatalf("test pop no wait not finished, try again")
	} else if errMsg != "" {
		t.Fatalf("test failed with error: %s", <-msg)
	}
}

func TestQueuePopWait(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	testItem := "string item"

	popedItem := make(chan *Item, 1)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		if item, err := q.Dequeue(); err == nil {
			t.Fatalf("expected empty queue but got %s from queue", item)
		}
		if item, err := q.Pop(true); err != nil {
			t.Fatalf("got error when poping item from queue: %s", err)
		} else {
			popedItem <- item
		}
		wg.Done()
	}()

	// wait 0.1s to push item into queue, Pop with blocking should return without error
	time.Sleep(100 * time.Millisecond)
	q.EnqueueObject(testItem)
	// wait til Pop finishes
	wg.Wait()
	select {
	case item := <-popedItem:
		assertEqualPopedObj(t, testItem, item)
	default:
		t.Fatalf("pop not success")
	}

	if q.Length() != 0 {
		t.Fatalf("expected empty queue but got queue with %d items", q.Length())
	}

	secondPush := make(chan bool)

	go func() {
		// push into queue before timeout, pop should get the item
		if item, err := q.Pop(true, 50*time.Millisecond); err != nil {
			t.Fatalf("pop should return item before timeout but got error: %s", err)
		} else {
			assertEqualPopedObj(t, testItem, item)
		}

		// this should hang here until main goroutine acknowledges that the second test starts
		<-secondPush
		if item, err := q.Pop(true, 50*time.Millisecond); item != nil {
			t.Fatalf("shouldn't pop out any item but got: %s", item)
		} else if err != ErrEmpty {
			t.Fatalf("got error when poping from queue: %s", err)
		}
	}()

	// test push before pop timeout
	time.Sleep(10 * time.Millisecond)
	q.EnqueueObject(testItem)

	// acknowledge pop goroutine ready for second test
	secondPush <- true

	// queue should be empty at the beginning of the second test
	if q.Length() != 0 {
		t.Fatalf("expected empty queue, but got %d items in the queue", q.Length())
	}

	// test push after pop timeout
	time.Sleep(100 * time.Millisecond)
	q.EnqueueObject(testItem)

	// queue should have one item inside
	if l := q.Length(); l != 1 {
		t.Fatalf("expected 1 item inside, but have %d", l)
	}

	time.Sleep(time.Second)
	if l := q.Length(); l != 1 {
		t.Fatalf("expected 1 item inside, but have %d", l)
	}

	if item, err := q.Dequeue(); err != nil {
		t.Fatalf("got error when poping from queue: %s", err)
	} else {
		assertEqualPopedObj(t, "string item", item)
	}
}

func TestQueueEnqueuePopConsistency(t *testing.T) {
	q, teardown := setupTestCase(t)
	defer teardown()

	// simultaneous goroutines cannot exceed 8192
	n := 4000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(j int) {
			time.Sleep(time.Duration(j*100) * time.Microsecond)
			q.EnqueueObject("string item")
			wg.Done()
		}(i)
	}

	// use channel count to avoid data race
	cnt := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			if item, err := q.Pop(true, time.Microsecond); err == nil {
				assertEqualPopedObj(t, "string item", item)
				cnt <- true
			} else if err != ErrEmpty {
				t.Fatalf("pop with error: %s", err)
			}
			wg.Done()
		}()

	}
	wg.Wait()
	if q.Length() != uint64(n-len(cnt)) {
		t.Fatalf("pop and enqueue is not consistent")
	}

}

func BenchmarkQueuePushObject(b *testing.B) {
	q, teardown := setupBenchmark(b)
	defer teardown()
	for i := 0; i < b.N; i++ {
		q.EnqueueObject("string item")
	}
}

func BenchmarkQueuePush(b *testing.B) {
	q, teardown := setupBenchmark(b)
	defer teardown()
	for i := 0; i < b.N; i++ {
		v, _ := json.Marshal("string item")
		q.Enqueue(v)
	}
}

/* Test Utils */

func setupTestCase(t *testing.T) (*Queue, func() error) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Fatalf("open queue failed with error %s", err)
	}
	return q, q.Drop
}

func setupBenchmark(b *testing.B) (*Queue, func() error) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		b.Fatalf("open queue failed with error %s", err)
	}
	return q, q.Drop
}

func assertEqualPopedObj(t *testing.T, expected interface{}, item *Item) {
	switch expected.(type) {
	case string:
		var actual string
		item.ToObject(&actual)
		if expected != actual {
			t.Fatalf("poped item does not match expectation, expected: %s, got: %s", expected, actual)
		}
	case int:
		var actual int
		item.ToObject(&actual)
		if expected != actual {
			t.Fatalf("poped item does not match expectation, expected: %s, got: %s", expected, actual)
		}
	default:
		t.Fatalf("unknown poped item type")
	}
}

func assertEqualPopedString(t *testing.T, expected string, item *Item) {
	if actual := item.ToString(); expected != actual {
		t.Fatalf("poped item does not match expectation, expected: %s, got: %s", expected, actual)
	}
}
