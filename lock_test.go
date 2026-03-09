package pglock

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/lib/pq"
)

// openDB creates a new database connection for testing.
func openDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("must set $DATABASE_URL to run tests")
		return nil
	}

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err, "failed to open database connection")
	require.NoError(t, db.Ping(), "failed to ping database")
	return db
}

// closeDB closes the database connection and reports any errors.
func closeDB(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Errorf("failed to close database connection: %v", err)
	}
}

func TestNewLock(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	t.Run("Valid", func(t *testing.T) {
		lock, err := New(context.Background(), 42, db)
		require.NoError(t, err, "failed to create lock")
		defer func() {
			assert.NoError(t, lock.Close(), "failed to close lock")
		}()

		assert.Equal(t, int64(42), lock.id)
		assert.NotNil(t, lock.conn)
	})

	t.Run("Canceled", func(t *testing.T) {
		// Cancel the context immediately
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		lock, err := New(ctx, 42, db)
		assert.Error(t, err, "expected error when creating lock with canceled context")
		assert.Nil(t, lock, "lock should be nil")
	})
}

func TestLock_BasicAcquisitionAndRelease(t *testing.T) {
	db1 := openDB(t)
	defer closeDB(t, db1)
	db2 := openDB(t)
	defer closeDB(t, db2)

	ctx := context.Background()
	id := int64(42)

	lock1, err := New(ctx, id, db1)
	require.NoError(t, err, "failed to create lock1 for db1")
	defer lock1.Close()

	lock2, err := New(ctx, id, db2)
	require.NoError(t, err, "failed to create lock2 for db2")
	defer lock2.Close()

	// lock1 should acquire the lock
	ok, err := lock1.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock1")
	require.True(t, ok, "should be able to lock lock1")

	// lock2 should not acquire the lock
	ok, err = lock2.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock2")
	require.False(t, ok, "should not be able to lock lock2")

	// Release lock1
	require.NoError(t, lock1.Unlock(ctx), "failed to unlock lock1")

	// lock2 should now acquire the lock
	ok, err = lock2.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock2")
	require.True(t, ok, "should be able to lock lock2")

	// Release lock2
	require.NoError(t, lock2.Unlock(ctx), "failed to unlock lock2")
}

func TestLock_SameSessionMultipleAcquisitionsAndReleases(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	ctx := context.Background()
	id := int64(42)

	lock, err := New(ctx, id, db)
	require.NoError(t, err, "failed to create lock")
	defer lock.Close()

	// Acquire the lock multiple times
	for i := 0; i < 8; i++ {
		ok, err := lock.TryLock(ctx)
		require.NoError(t, err, "failed to try lock lock")
		require.True(t, ok, "should be able to lock lock")
	}

	// Release the lock multiple times
	for i := 0; i < 8; i++ {
		require.NoError(t, lock.Unlock(ctx), "failed to unlock lock")
	}
}

func TestLock_UnlockNotHeld(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	ctx := context.Background()
	id := int64(42)

	lock, err := New(ctx, id, db)
	require.NoError(t, err, "failed to create lock")
	defer lock.Close()

	require.NoError(t, lock.Unlock(ctx), "should be able to unlock unheld lock without error")
}

func TestLock_Stacking(t *testing.T) {
	db1 := openDB(t)
	defer closeDB(t, db1)
	db2 := openDB(t)
	defer closeDB(t, db2)

	ctx := context.Background()
	id := int64(42)

	lock1, err := New(ctx, id, db1)
	require.NoError(t, err, "failed to create lock1 for db1")
	defer lock1.Close()

	lock2, err := New(ctx, id, db2)
	require.NoError(t, err, "failed to create lock2 for db2")
	defer lock2.Close()

	// lock1 should acquire the lock multiple times
	for i := 0; i < 8; i++ {
		ok, err := lock1.TryLock(ctx)
		require.NoError(t, err, "failed to try lock lock1")
		require.True(t, ok, "should be able to lock lock1")
	}

	// Lock2 should not be able to acquire the lock while multiple locks are held
	for i := 0; i < 8; i++ {
		ok, err := lock2.TryLock(ctx)
		require.NoError(t, err, "failed to try lock lock2")
		require.False(t, ok, "should not be able to lock lock2")

		require.NoError(t, lock1.Unlock(ctx), "failed to unlock lock1")
	}

	// Now that lock1 has been unlocked, lock2 should be able to acquire the lock
	ok, err := lock2.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock2")
	require.True(t, ok, "should be able to lock lock2")

	// Release lock2
	require.NoError(t, lock2.Unlock(ctx), "failed to unlock lock2")
}

func TestLock_NegativeID(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	ctx := context.Background()
	id := int64(-42)

	lock, err := New(ctx, id, db)
	require.NoError(t, err, "failed to create lock")
	defer lock.Close()

	ok, err := lock.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock")
	require.True(t, ok, "should be able to lock lock")

	require.NoError(t, lock.Unlock(ctx), "failed to unlock lock")

	ok, err = lock.TryRLock(ctx)
	require.NoError(t, err, "failed to try rlock lock")
	require.True(t, ok, "should be able to rlock lock")

	require.NoError(t, lock.RUnlock(ctx), "failed to runlock lock")

	err = lock.Lock(ctx)
	require.NoError(t, err, "should be able to lock negative lock")
	require.NoError(t, lock.Unlock(ctx), "failed to unlock lock")

	err = lock.RLock(ctx)
	require.NoError(t, err, "should be able to rlock negative lock")
	require.NoError(t, lock.RUnlock(ctx), "failed to unlock lock")
}

func TestLock_Blocking(t *testing.T) {
	db1 := openDB(t)
	defer closeDB(t, db1)
	db2 := openDB(t)
	defer closeDB(t, db2)

	ctx := context.Background()
	id := int64(42)

	lock1, err := New(ctx, id, db1)
	require.NoError(t, err, "failed to create lock1 for db1")
	defer lock1.Close()

	lock2, err := New(ctx, id, db2)
	require.NoError(t, err, "failed to create lock2 for db2")
	defer lock2.Close()

	var (
		wg sync.WaitGroup
		mu atomic.Bool
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := lock1.Lock(ctx)
		require.NoError(t, err, "failed to lock lock1")
		defer lock1.Unlock(ctx)
		time.Sleep(500 * time.Millisecond)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(250 * time.Millisecond)
		err := lock2.Lock(ctx)
		require.NoError(t, err, "failed to lock lock2")
		defer lock2.Unlock(ctx)
		mu.Store(true)
	}()

	wg.Wait()
	require.True(t, mu.Load(), "lock2 should have been able to lock")
}

func TestLock_BlockingWithContextCancel(t *testing.T) {
	db1 := openDB(t)
	defer closeDB(t, db1)
	db2 := openDB(t)
	defer closeDB(t, db2)

	var (
		wg sync.WaitGroup
		mu atomic.Bool
		id int64 = 42
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		lock1, err := New(ctx, id, db1)
		require.NoError(t, err, "failed to create lock1 for db1")
		defer lock1.Close()

		err = lock1.Lock(ctx)
		require.NoError(t, err, "failed to lock lock1")
		defer lock1.Unlock(ctx)
		time.Sleep(500 * time.Millisecond)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lock2, err := New(context.Background(), id, db2)
		require.NoError(t, err, "failed to create lock2 for db2")
		defer lock2.Close()

		time.Sleep(100 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err = lock2.Lock(ctx)
		require.Error(t, err, "expected context cancelled error")

		if err == nil {
			mu.Store(true)
		}

	}()

	wg.Wait()
	require.False(t, mu.Load(), "lock2 should not have been able to lock")
}

func TestLock_Concurrency(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	var (
		wg      sync.WaitGroup
		id      int64 = 42
		counter int64 = 0
	)

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lock, err := New(context.Background(), id, db)
			require.NoError(t, err, "failed to create lock")
			defer lock.Close()

			// Acquire the lock
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			if err := lock.Lock(ctx); err != nil {
				t.Errorf("failed to lock lock: %v", err)
				return
			}

			// Critical section
			current := atomic.LoadInt64(&counter)
			time.Sleep(time.Duration(rand.Intn(20))*time.Millisecond + (5 * time.Millisecond))
			atomic.StoreInt64(&counter, current+1)

			if err := lock.Unlock(ctx); err != nil {
				t.Errorf("failed to unlock lock: %v", err)
				return
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(16), atomic.LoadInt64(&counter), "counter should be 16")
}

func TestLock_CloseReleasesLocks(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	ctx := context.Background()
	id := int64(42)

	lock, err := New(ctx, id, db)
	require.NoError(t, err, "failed to create lock")

	for i := 0; i < 16; i++ {
		err := lock.Lock(ctx)
		require.NoError(t, err, "failed to lock lock")
	}

	require.NoError(t, lock.Close(), "failed to close lock")

	lock, err = New(ctx, id, db)
	require.NoError(t, err, "failed to create lock")
	defer lock.Close()

	// Should be able to lock with the new lock
	ok, err := lock.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock")
	require.True(t, ok, "should be able to lock lock")

	// Should be able to unlock with the new lock
	require.NoError(t, lock.Unlock(ctx), "failed to unlock lock")
}

func TestLock_DifferentIDs(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	ctx := context.Background()
	lock1, err := New(ctx, int64(42), db)
	require.NoError(t, err, "failed to create lock1")
	defer lock1.Close()

	lock2, err := New(ctx, int64(43), db)
	require.NoError(t, err, "failed to create lock2")
	defer lock2.Close()

	// Should be able to lock with the same ID
	ok, err := lock1.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock1")
	require.True(t, ok, "should be able to lock lock1")

	// Should be able to lock with the different ID
	ok, err = lock2.TryLock(ctx)
	require.NoError(t, err, "failed to try lock lock2")
	require.True(t, ok, "should be able to lock lock2")

	// Should be able to unlock with the same ID
	require.NoError(t, lock1.Unlock(ctx), "failed to unlock lock1")

	// Should be able to unlock with the different ID
	require.NoError(t, lock2.Unlock(ctx), "failed to unlock lock2")
}

func TestRLock_BasicAcquisitionAndRelease(t *testing.T) {
	dbs := make([]*sql.DB, 8)
	for i := range dbs {
		dbs[i] = openDB(t)
		defer closeDB(t, dbs[i])
	}

	ctx := context.Background()
	id := int64(42)

	locks := make([]*Lock, len(dbs))
	for i := range locks {
		var err error
		locks[i], err = New(ctx, id, dbs[i])
		require.NoError(t, err, "failed to create lock for db %d", i)
		defer locks[i].Close()
	}

	// Should be able to acuire read locks from multiple sessions
	for i := range locks {
		ok, err := locks[i].TryRLock(ctx)
		require.NoError(t, err, "failed to try rlock lock %d", i)
		require.True(t, ok, "should be able to rlock lock %d", i)
	}

	// Should be able to release read locks from multiple sessions
	for i := range locks {
		require.NoError(t, locks[i].RUnlock(ctx), "failed to runlock lock %d", i)
	}
}

func TestRlock_Blocking(t *testing.T) {
	db1 := openDB(t)
	defer closeDB(t, db1)
	db2 := openDB(t)
	defer closeDB(t, db2)

	ctx := context.Background()
	id := int64(42)

	lock1, err := New(ctx, id, db1)
	require.NoError(t, err, "failed to create lock1 for db1")
	defer lock1.Close()

	lock2, err := New(ctx, id, db2)
	require.NoError(t, err, "failed to create lock2 for db2")
	defer lock2.Close()

	var (
		wg sync.WaitGroup
		mu atomic.Bool
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := lock1.Lock(ctx)
		require.NoError(t, err, "failed to lock lock1")
		defer lock1.Unlock(ctx)
		time.Sleep(250 * time.Millisecond)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := lock2.RLock(ctx)
		require.NoError(t, err, "failed to rlock lock2")
		defer lock2.RUnlock(ctx)

		mu.Store(true)
	}()

	wg.Wait()
	require.True(t, mu.Load(), "lock2 should have been able to rlock")
}

func TestRlock_BlockingWithContextCancel(t *testing.T) {
	db1 := openDB(t)
	defer closeDB(t, db1)
	db2 := openDB(t)
	defer closeDB(t, db2)

	var (
		wg sync.WaitGroup
		mu atomic.Bool
		id int64 = 42
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.Background()
		lock1, err := New(ctx, id, db1)
		require.NoError(t, err, "failed to create lock1 for db1")
		defer lock1.Close()

		err = lock1.Lock(ctx)
		require.NoError(t, err, "failed to lock lock1")
		defer lock1.Unlock(ctx)
		time.Sleep(250 * time.Millisecond)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		lock2, err := New(ctx, id, db2)
		require.NoError(t, err, "failed to create lock2 for db2")
		defer lock2.Close()

		err = lock2.RLock(ctx)
		require.Error(t, err, "expected context cancelled error")

		if err == nil {
			mu.Store(true)
		}
	}()

	wg.Wait()
	require.False(t, mu.Load(), "lock2 should not have been able to rlock")
}

func TestRlock_Concurrency(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	var (
		wg      sync.WaitGroup
		id      int64 = 42
		counter int64 = 0
	)

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lock, err := New(context.Background(), id, db)
			require.NoError(t, err, "failed to create lock")
			defer lock.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			if err := lock.RLock(ctx); err != nil {
				t.Errorf("failed to rlock lock: %v", err)
				return
			}

			// This is technically only safe since we're using atomic, the rlocks
			// don't actually make it safe - but it's enough to test that the rlocks
			// are shared and not blocking.
			atomic.AddInt64(&counter, 1)

			if err := lock.RUnlock(ctx); err != nil {
				t.Errorf("failed to runlock lock: %v", err)
				return
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(16), atomic.LoadInt64(&counter), "counter should be 16")
}
