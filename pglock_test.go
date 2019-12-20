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
	defaultTimeout = 50 * time.Millisecond
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

		time.Sleep(defaultTimeout / 2)

		assert.True(t, l.IsLocked())
		l.Unlock(defaultTimeout)
		assert.False(t, l.IsLocked())
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

			assert.True(t, l.IsLocked())
			l.Unlock(defaultTimeout)
			assert.False(t, l.IsLocked())
			done <- true
		}()
	} else {
		t.Error("Not locked by the first goroutine")
		return
	}

	if <-done {
		t1 := <-times
		t2 := <-times

		assert.WithinDuration(t, t1, t2, defaultTimeout)
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
		assert.True(t, l.IsLocked())

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
			assert.False(t, l.IsLocked())
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
		assert.True(t, l.IsLocked())
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
		assert.False(t, l.IsLocked())
	}
}

func TestTryLockSucceed(t *testing.T) {
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

		time.Sleep(defaultTimeout / 2)

		assert.True(t, l.IsLocked())
		l.Unlock(defaultTimeout)
		assert.False(t, l.IsLocked())
	}()

	if <-locked {
		go func() {
			l := getLock(t, testDB)
			locked, err := l.TryLock(defaultTimeout)
			if err != nil {
				done <- false
				t.Fatal(err)
			}
			if assert.False(t, locked) {
				assert.False(t, l.IsLocked())

				time.Sleep(defaultTimeout)

				locked, err = l.TryLock(defaultTimeout)
				if err != nil {
					done <- false
					t.Fatal(err)
				}
				if assert.True(t, locked) {
					times <- time.Now()
					assert.True(t, l.IsLocked())
					l.Unlock(defaultTimeout)
					assert.False(t, l.IsLocked())
					done <- true
				} else {
					done <- false
				}
			} else {
				done <- false
			}
		}()
	} else {
		t.Error("Not locked by the first goroutine")
		return
	}

	if <-done {
		t1 := <-times
		t2 := <-times

		assert.WithinDuration(t, t1, t2, time.Duration(float64(defaultTimeout)*1.5))
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
		assert.False(t, l2.IsLocked())
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
