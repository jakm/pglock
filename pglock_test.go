package pglock

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const (
	lockID         = "lock-id"
	defaultTimeout = 10 * time.Millisecond
)

var (
	connStr string
	testDB  *sql.DB
)

func TestMain(m *testing.M) {
	connStr = os.Getenv("POSTGRESQL_URL")
	if connStr == "" {
		panic("Empty POSTGRESQL_URL environment variable")
	}

	var err error
	testDB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer testDB.Close()

	os.Exit(m.Run())
}

func TestLockSucceed(t *testing.T) {
	locked := make(chan bool)
	done := make(chan bool)
	times := make(chan time.Time, 2)

	go func() {
		l := getLock(t)
		err := l.Lock(defaultTimeout)
		if err != nil {
			locked <- false
			t.Fatal()
		}
		locked <- true
		times <- time.Now()
		time.Sleep(5 * time.Millisecond)
		l.Unlock(defaultTimeout)
	}()

	if <-locked {
		go func() {
			l := getLock(t)
			err := l.Lock(defaultTimeout)
			if err != nil {
				done <- false
				t.Fatal(err)
			}
			times <- time.Now()
			l.Unlock(defaultTimeout)
			done <- true
		}()
	} else {
		t.Error("Not locked by the first goroutine")
		return
	}

	if <-done {
		t1 := <-times
		t2 := <-times

		assert.WithinDuration(t, t1, t2, 10*time.Millisecond)
	}
}

func TestLockTimeout(t *testing.T) {
	locked := make(chan bool)
	done := make(chan bool, 2)

	go func() {
		l := getLock(t)
		err := l.Lock(defaultTimeout)
		if err != nil {
			locked <- false
			t.Fatal()
		}
		locked <- true
		time.Sleep(2 * defaultTimeout)
		err = l.Unlock(defaultTimeout)
		assert.NoError(t, err)
		done <- true
	}()

	if <-locked {
		go func() {
			l := getLock(t)
			err := l.Lock(defaultTimeout)
			assert.EqualError(t, err, "Timed out")
			done <- true
		}()
	} else {
		t.Error("Not locked by the first goroutine")
		return
	}

	<-done
	<-done
}

func TestAlreadyLockedError(t *testing.T) {
	l := getLock(t)
	err := l.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer l.Unlock(defaultTimeout)
		err = l.Lock(defaultTimeout)
		assert.EqualError(t, err, "Already locked")
	}
}

func TestAlreadyUnlockedError(t *testing.T) {
	l := getLock(t)
	err := l.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		err = l.Unlock(defaultTimeout)
		assert.NoError(t, err)
		err = l.Unlock(defaultTimeout)
		assert.EqualError(t, err, "Already unlocked")
	}
}

func TestTryLockReturns(t *testing.T) {
	l1 := getLock(t)
	err := l1.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer l1.Unlock(defaultTimeout)
		l2 := getLock(t)
		locked, err := l2.TryLock(defaultTimeout)
		assert.False(t, locked)
		assert.NoError(t, err)
	}
}

func TestUnlockTimeout(t *testing.T) {
	l := getLock(t)
	err := l.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		err = l.Unlock(0)
		assert.EqualError(t, err, "Timed out")
		l.Unlock(defaultTimeout)
	}
}

func TestConcurrentConnections(t *testing.T) {
	anotherConn, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer anotherConn.Close()

	l1 := getLock(t)
	err = l1.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer func() {
			if l1.IsLocked() {
				l1.Unlock(defaultTimeout)
			}
		}()

		l2, err := NewAdvisoryLock(anotherConn, lockID)
		if err != nil {
			t.Fatal(err)
		}

		err = l2.Lock(defaultTimeout)
		assert.EqualError(t, err, "Timed out")

		locked, err := l2.TryLock(defaultTimeout)
		assert.False(t, locked)
		assert.NoError(t, err)

		l1.Unlock(defaultTimeout)

		err = l2.Lock(defaultTimeout)
		assert.NoError(t, err)

		err = l2.Unlock(defaultTimeout)
		assert.NoError(t, err)
	}
}

func getLock(t *testing.T) *AdvisoryLock {
	l, err := NewAdvisoryLock(testDB, lockID)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
