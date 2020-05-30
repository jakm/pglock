package pglock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/subchen/go-trylock"
	"hash/fnv"
	"time"
)

const salt = "9ad09a3a30492944c3b62687101ca58e"

var (
	ErrAlreadyLocked = errors.New("Already locked")
	ErrNotLocked     = errors.New("Not locked")
	ErrTimeout       = errors.New("Timed out")
)

type AdvisoryLock struct {
	id       int
	db       *sql.DB
	conn     *sql.Conn
	mutex    trylock.TryLocker
	pgLocked bool
}

func NewAdvisoryLock(db *sql.DB, id string) (*AdvisoryLock, error) {
	h, err := hashID(id)
	if err != nil {
		return nil, err
	}
	return &AdvisoryLock{db: db, id: h, mutex: trylock.New()}, nil
}

func hashID(id string) (int, error) {
	h := fnv.New64()
	_, err := h.Write([]byte(salt + id))
	if err != nil {
		return 0, err
	}
	return int(h.Sum64()), nil
}

func (l *AdvisoryLock) IsLocked() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.pgLocked
}

func (l *AdvisoryLock) TryLock(timeout time.Duration) (bool, error) {
	return l.lock(timeout, false)
}

func (l *AdvisoryLock) Lock(timeout time.Duration) error {
	_, err := l.lock(timeout, true)
	return err
}

func (l *AdvisoryLock) lock(timeout time.Duration, blocking bool) (success bool, err error) {
	defer func() {
		if err == context.DeadlineExceeded {
			err = ErrTimeout
		}
	}()

	deadline := time.Now().Add(timeout)

	if blocking {
		locked := l.mutex.TryLock(timeout)
		if !locked {
			return false, ErrTimeout
		}
	} else {
		locked := l.mutex.TryLock(0)
		if !locked {
			return
		}
	}
	defer l.mutex.Unlock()

	if l.pgLocked {
		return false, ErrAlreadyLocked
	}

	ctx, cancel := getContext(deadline)
	defer cancel()

	// make sure the same connection is used for both locking and unlocking
	// https://engineering.qubecinema.com/2019/08/26/unlocking-advisory-locks.html
	l.conn, err = l.db.Conn(ctx)
	if err != nil {
		return
	}

	if blocking {
		var tx *sql.Tx
		tx, err = l.conn.BeginTx(ctx, nil)
		if err != nil {
			return
		}
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`SET LOCAL lock_timeout = '%s'`, getPgLockTimeout(deadline)))
		if err != nil {
			return
		}
		_, err = tx.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, l.id)
		if err == nil {
			l.pgLocked = true
			err = tx.Commit()
		}
		if err != nil {
			if isLockNotAvailableError(err) {
				return false, ErrTimeout
			}
			return
		}
	} else {
		err = l.conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, l.id).Scan(&l.pgLocked)
		if err != nil {
			if isQueryCanceledError(err) {
				return false, ErrTimeout
			}
			return
		}
	}

	return l.pgLocked, nil
}

func getPgLockTimeout(deadline time.Time) string {
	d := deadline.Sub(time.Now())
	if d < time.Millisecond {
		return time.Millisecond.String()
	}
	return d.String()
}

func (l *AdvisoryLock) Unlock(timeout time.Duration) (err error) {
	defer func() {
		if err == context.DeadlineExceeded {
			err = ErrTimeout
		}
	}()

	deadline := time.Now().Add(timeout)

	locked := l.mutex.TryLock(timeout)
	if !locked {
		return ErrTimeout
	}
	defer l.mutex.Unlock()

	if !l.pgLocked {
		return ErrNotLocked
	}

	ctx, cancel := getContext(deadline)
	defer cancel()

	_, err = l.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, l.id)
	if err != nil {
		if isQueryCanceledError(err) {
			return ErrTimeout
		}
		return
	}

	l.conn.Close()
	l.conn = nil
	l.pgLocked = false

	return nil
}

func getContext(deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.Background(), deadline)
}

func isQueryCanceledError(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		// 57014 is the postgres error code for canceled queries
		if err.Code == "57014" {
			return true
		}
	}
	return false
}

func isLockNotAvailableError(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		// 55P03 is the postgres error code for timed out locks
		if err.Code == "55P03" {
			return true
		}
	}
	return false
}
