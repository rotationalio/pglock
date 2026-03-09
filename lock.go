/*
Package pglock provides distributed locking using PostgreSQL session advisory locks.

PostgreSQL advisory locks are application-defined locks used to manage concurrency and
synchronize custom processes rather than locking database rows or tables. They are
advisory-only, meaning that the database does not enforce the locking of schema objects
only access to the locks. Advisory locks are faster than table-based locks, avoid
table bloat, and are automatically cleaned up by the postgres server at the end of a
session (when the connection is closed).

Session advisory locks are held until explicitly released or until the session ends.
Unlike transaction-level advisory locks, session-level locks do not honor transaction
semantics and a transaction that creates a session-level lock and is rolled back will
not automatically release the lock.

This package provides a simple interface for acquiring and releasing session advisory
in the same way that you might use a mutex in a single-process application.

NOTE: this is package is a fork of the github.com/allisson/go-pglock package with a
slight re-working of the API to better fit the needs of the Rotational team.
*/
package pglock

import (
	"context"
	"database/sql"
)

// A Lock holds a dedicated database connection and a lock identifier. The connection is
// obtained from the connection pool and is held for the lifetime of the Lock instance
// to maintain the session-level advisory lock. Note that a lock is not thread-safe and
// should not be shared between goroutines.
type Lock struct {
	id   int64
	conn *sql.Conn
}

// Create a new Lock instance with a dedicated database connection.
//
// This function obtains a connection from the provided database connection pool and
// stores it for use in lock and unlock operations. The connection is held for the
// lifetime of the Lock instance to maintain the session-level advisory lock.
//
// The context is used for managing the connection acquisition. The lock identifier is
// used as the postgres advisory lock key, and the db is a connection pool from which
// to obtain a dedicated connection (session) for the lock.
//
// The caller is responsible for calling Close on the returned Lock to release the
// connection back to the pool and clean up any held advisory locks.
//
// An error is returned if the connection cannot be obtained from the pool.
func New(ctx context.Context, id int64, db *sql.DB) (lock Lock, err error) {
	lock = Lock{id: id}

	if lock.conn, err = db.Conn(ctx); err != nil {
		return Lock{}, err
	}

	return lock, nil
}

// Close the database connection, releasing all advisory locks held by the lock.
//
// Since advisory locks are automatically cleaned up with a database session ends,
// closing the connection will release all locks held, regardless of how many times
// they were acquired. This provides a reliable way to ensure all locks are released
// when the Lock instance is no longer needed.
//
// After calling Close, an ErrConnDone will be returned from any subsequent method calls.
func (l *Lock) Close() error {
	return l.conn.Close()
}

const tryLockSQL = "SELECT pg_try_advisory_lock($1)"

// TryLock attempts to obtain an exclusive session level advisory lock without waiting.
//
// This method uses PostgreSQL's pg_try_advisory_lock function, which doesn't block;
// it will either obtain the lock immediately and return true, or return false if the
// lock is already held by another session.
//
// Multiple lock requests stack within the same session, meaning if a resource is locked
// three times, it must be unlocked three times to be fully released.
func (l *Lock) TryLock(ctx context.Context) (result bool, err error) {
	err = l.conn.QueryRowContext(ctx, tryLockSQL, l.id).Scan(&result)
	return result, err
}

const tryRLockSQL = "SELECT pg_try_advisory_lock_shared($1)"

// TryRLock attempts to obtain a shared session level advisory lock without waiting.
//
// This method uses PostgreSQL's pg_try_advisory_lock_shared function, which doesn't
// block; it will either obtain the shared lock immediately and return true, or return
// false if an exclusive lock is already held by another session.
//
// Shared locks are ideal for read operations where multiple readers can safely access
// a resource concurrently, but writers need to be prevented from modifying the resource
// while its being read.
//
// Multiple lock requests stack within the same session, meaning if a resource is locked
// three times, it must be unlocked three times to be fully released.
func (l *Lock) TryRLock(ctx context.Context) (result bool, err error) {
	err = l.conn.QueryRowContext(ctx, tryRLockSQL, l.id).Scan(&result)
	return result, err
}

const lockSQL = "SELECT pg_advisory_lock($1)"

// Lock attempts to obtain an exclusive session level advisory lock, blocking until
// either the lock is obtained or the context is cancelled.
//
// This method uses PostgreSQL's pg_advisory_lock function, which blocks until the lock
// is obtained. If another session already holds a lock on the same identifier, this
// method will wait until the lock is available.
//
// Multiple lock requests stack within the same session, meaning if a resource is locked
// three times, it must be unlocked three times to be fully released. If the session
// already holds the given advisory lock, additional requests will always succeed
// immediately without blocking.
func (l *Lock) Lock(ctx context.Context) (err error) {
	_, err = l.conn.ExecContext(ctx, lockSQL, l.id)
	return err
}

const rLockSQL = "SELECT pg_advisory_lock_shared($1)"

// RLock attempts to obtain a shared session level advisory lock, blocking until
// either the lock is obtained or the context is cancelled.
//
// This method uses PostgreSQL's pg_advisory_lock_shared function, which blocks until
// the lock is obtained. If another session already holds an exclusive lock on the same
// identifier, this method will wait until the exclusive lock is released.
//
// Multiple lock requests stack within the same session, meaning if a resource is locked
// three times, it must be unlocked three times to be fully released. Multiple shared
// locks can be acquired by multiple sessions, meaning shared locks are ideal for read
// operations where multiple sessions must read the same resource concurrently without
// modification from other sessions.
func (l *Lock) RLock(ctx context.Context) (err error) {
	_, err = l.conn.ExecContext(ctx, rLockSQL, l.id)
	return err
}

const unlockSQL = "SELECT pg_advisory_unlock($1)"

// Unlock releases a previously acquired session level advisory lock, allowing other
// sessions to acquire it.
//
// This method uses PostgreSQL's pg_advisory_unlock function, which releases one level
// of lock ownership. Because lock requests stack within a session, each Unlock call
// only decrements the lock count by one. If the same lock is acquired multiple times,
// it must be unlocked the same number of times to be fully released.
//
// Unlocking a lock not currently held will not return an error, but it may have
// unexpected side effects in PostgreSQL. It is the caller's responsibility to ensure
// that the lock is actually held before attempting to unlock it.
func (l *Lock) Unlock(ctx context.Context) (err error) {
	_, err = l.conn.ExecContext(ctx, unlockSQL, l.id)
	return err
}

const rUnlockSQL = "SELECT pg_advisory_unlock_shared($1)"

// RUnlock releases a previously acquired shared session level advisory lock, allowing
// other sessions to acquire it.
//
// This method uses PostgreSQL's pg_advisory_unlock_shared function, which releases one
// level of shared lock ownership. Because shared lock requests stack within a session,
// each RUnlock call only decrements the shared lock count by one. If the same shared
// lock is acquired multiple times, it must be unlocked the same number of times to be
// fully released.
//
// Unlocking a shared lock not currently held will not return an error, but it may have
// unexpected side effects in PostgreSQL. It is the caller's responsibility to ensure
// that the shared lock is actually held before attempting to unlock it.
func (l *Lock) RUnlock(ctx context.Context) (err error) {
	_, err = l.conn.ExecContext(ctx, rUnlockSQL, l.id)
	return err
}
