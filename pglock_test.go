package pglock

import (
	"context"
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
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

	testDB = getDBConn()
	defer testDB.Close()

	os.Exit(m.Run())
}

func TestLockSucceed(t *testing.T) {
	locked := make(chan bool)
	done := make(chan bool)
	times := make(chan time.Time, 2)

	go func() {
		l := getLock(t, testDB)
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
			l := getLock(t, testDB)
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
		l := getLock(t, testDB)
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
			l := getLock(t, testDB)
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
	l := getLock(t, testDB)
	err := l.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer l.Unlock(defaultTimeout)
		err = l.Lock(defaultTimeout)
		assert.EqualError(t, err, "Already locked")
	}
}

func TestNotLockedError(t *testing.T) {
	l := getLock(t, testDB)
	err := l.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		err = l.Unlock(defaultTimeout)
		assert.NoError(t, err)
		err = l.Unlock(defaultTimeout)
		assert.EqualError(t, err, "Not locked")
	}
}

func TestTryLockReturns(t *testing.T) {
	l1 := getLock(t, testDB)
	err := l1.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer l1.Unlock(defaultTimeout)
		l2 := getLock(t, testDB)
		locked, err := l2.TryLock(defaultTimeout)
		assert.False(t, locked)
		assert.NoError(t, err)
	}
}

func TestUnlockTimeout(t *testing.T) {
	t.Run("DeadlineExceeded", func(t *testing.T) {
		testUnlockTimeout(t, context.DeadlineExceeded)
	})
	t.Run("QueryCanceled", func(t *testing.T) {
		testUnlockTimeout(t, &pq.Error{Code: "57014"})
	})
}

func testUnlockTimeout(t *testing.T, returnedErr error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	h, err := hashID(lockID)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(h).WillReturnRows(sqlmock.NewRows([]string{"pg_try_advisory_lock"}).AddRow(true))
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(h).WillReturnError(context.DeadlineExceeded)

	l := getLock(t, db)
	_, err = l.TryLock(defaultTimeout)
	if assert.NoError(t, err) {
		err = l.Unlock(defaultTimeout)
		assert.EqualError(t, err, "Timed out")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
	}
}

func TestConcurrentConnections(t *testing.T) {
	anotherConn := getDBConn()
	defer anotherConn.Close()

	l1 := getLock(t, testDB)
	err := l1.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer func() {
			if l1.IsLocked() {
				l1.Unlock(defaultTimeout)
			}
		}()

		l2 := getLock(t, anotherConn)

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

func TestConcurrentUnlockError(t *testing.T) {
	anotherConn := getDBConn()
	defer anotherConn.Close()

	l1 := getLock(t, testDB)
	err := l1.Lock(defaultTimeout)
	if assert.NoError(t, err) {
		defer func() {
			if l1.IsLocked() {
				l1.Unlock(defaultTimeout)
			}
		}()

		l2 := getLock(t, anotherConn)

		err = l2.Unlock(defaultTimeout)
		assert.EqualError(t, err, "Not locked")
	}
}

func getDBConn() *sql.DB {
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	return conn
}

func getLock(t *testing.T, db *sql.DB) *AdvisoryLock {
	l, err := NewAdvisoryLock(db, lockID)
	if err != nil {
		t.Fatal(err)
	}
	return l
}
