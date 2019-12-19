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
	ErrAlreadyLocked  = errors.New("Already locked")
	ErrAlreadyUnocked = errors.New("Already unlocked")
	ErrTimeout        = errors.New("Timed out")
)

type AdvisoryLock struct {
	id       int
	db       *sql.DB
	conn     *sql.Conn
	mutex    trylock.TryLocker
	pgLocked bool
}

func NewAdvisoryLock(db *sql.DB, id string) (*AdvisoryLock, error) {
	h := fnv.New64()
	_, err := h.Write([]byte(salt + id))
	if err != nil {
		return nil, err
	}
	s := int(h.Sum64())

	return &AdvisoryLock{db: db, id: s, mutex: trylock.New()}, nil
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
	deadline := time.Now().Add(timeout)

	if blocking {
		l.mutex.Lock()
	} else {
		locked := l.mutex.TryLock(timeout)
		if !locked {
			return
		}
	}
	defer l.mutex.Unlock()

	if l.pgLocked {
		return false, ErrAlreadyLocked
	}

	if isTimeout(blocking, deadline) {
		return false, ErrTimeout
	}

	ctx, cancel := getContext(blocking, deadline)
	defer cancel()

	// make sure the same connection is used for both locking and unlocking
	// https://engineering.qubecinema.com/2019/08/26/unlocking-advisory-locks.html
	l.conn, err = l.db.Conn(ctx)
	if err != nil {
		return
	}

	if blocking {
		var tx *sql.Tx
		tx, err = l.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return
		}
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`SET LOCAL lock_timeout = '%s'`, deadline.Sub(time.Now())))
		if err != nil {
			return false, err
		}
		_, err = l.conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, l.id)
		if err != nil {
			if isLockNotAvailableError(err) {
				return false, ErrTimeout
			}
			return
		}
		l.pgLocked = true
		tx.Commit()
	} else {
		err = l.conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, l.id).Scan(&l.pgLocked)
		if err != nil {
			if isQueryCancelledError(err) {
				return false, ErrTimeout
			}
			return
		}
	}

	return l.pgLocked, nil
}

func (l *AdvisoryLock) Unlock(timeout time.Duration) error {
	return l.unlock(timeout, false)
}

func (l *AdvisoryLock) unlock(timeout time.Duration, blocking bool) error {
	deadline := time.Now().Add(timeout)

	if blocking {
		l.mutex.Lock()
	} else {
		l.mutex.TryLock(timeout)
	}
	defer l.mutex.Unlock()

	if !l.pgLocked {
		return ErrAlreadyUnocked
	}

	if isTimeout(blocking, deadline) {
		return ErrTimeout
	}

	defer func() {
		l.conn.Close()
		l.conn = nil
		l.pgLocked = false
	}()

	ctx, cancel := getContext(blocking, deadline)
	defer cancel()

	_, err := l.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, l.id)
	if err != nil {
		if isQueryCancelledError(err) {
			return nil
		}
		return err
	}

	return nil
}

func isTimeout(blocking bool, deadline time.Time) bool {
	if !blocking {
		t := time.Until(deadline)
		if t <= 0 {
			return true
		}
	}
	return false
}

func getContext(blocking bool, deadline time.Time) (context.Context, context.CancelFunc) {
	if !blocking {
		return context.WithDeadline(context.Background(), deadline)
	}
	return context.Background(), func() {}
}

func isQueryCancelledError(err error) bool {
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
