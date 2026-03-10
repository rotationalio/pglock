# pglock

[![CI Tests](https://github.com/rotationalio/pglock/actions/workflows/tests.yaml/badge.svg)](https://github.com/rotationalio/pglock/actions/workflows/tests.yaml)
[![Go Report Card](https://goreportcard.com/badge/go.rtnl.ai/pglock)](https://goreportcard.com/report/go.rtnl.ai/pglock)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/go.rtnl.ai/pglock)

**Distributed locking using PostgreSQL session level advisory locks**

This repo is a port and modification of [github.com/allisson/go-pglock](https://github.com/allisson/go-pglock) and per its MIT license, has a similar MIT license with the copyright included.

`pglock` provides a database-backed solution to implement distributed locks across multiple processes using PostgreSQL's [advisory lock mechanism](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS). You should use `pglock` when you're trying to coordinate access to shared resources and do not want to use a distributed consistency mechanism. The point of failure is the PostgreSQL database, but so long as it remains up and stable, locking should be reliable.

## Installation

```sh
go get go.rtnl.ai/pglock
```

Requirements:

- Go 1.25 or higher
- PostgreSQL 15 or higher

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"

    "go.rtnl.ai/pglock"
    _ "github.com/lib/pq"
)

func main() {
    // Connect to PostgreSQL
    db, err := sql.Open("postgres", "postgres://user:pass@localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Create a lock with a unique ID
    lock, err := pglock.New(ctx, pglock.Hash("mylock"), db)
    if err != nil {
        log.Fatal(err)
    }

    // Try to acquire the lock
    acquired, err := log.TryLock(ctx)
    if err != nil {
        log.Fatal(err)
    }

    if acquired {
        fmt.Println("Lock acquired! Doing work ...")

        // Release the lock
        if err := lock.Unlock(ctx); err != nil {
            log.Fatal(err)
        }
        fmt.Println("Lock released!")
    } else {
        fmt.Println("Could not acquire lock - another process has it")
    }
}
```

## How it Works

PostgreSQL advisory locks enable powerful features for application defined distributed locking:

- **Session-level locks**: Locks are held until explicitly released or the database connection closes.
- **Application-defined**: Locks are identified by a numeric identifier that is specific to your application and use case.
- **Fast and efficient**: No table bloat, faster than row-level locks
- **Lock stacking**: A session can acquire the same lock multiple times (requires equal number of unlocks).
- **Automatic cleanup**: PostgreSQL automatically releases locks when the session (database connection) ends.

From the PostgreSQL Documentation:

> PostgreSQL provides a means for creating locks that have application-defined meanings. These are called advisory locks, because the system does not enforce their use — it is up to the application to use them correctly. Advisory locks can be useful for locking strategies that are an awkward fit for the MVCC model.

## API

### `New(ctx context.Context, id int64, db *sql.DB) (*Lock, error)`

Creates a new lock instance with a dedicated database connection.

### `Hash(s string) int64`

Generates a 64-bit FNV-1a hash of the input string, allowing you to create integer IDs from more memorable human-readable strings that are easier to share between processes.

### `Lock.Lock(ctx context.Context) error`

Attempts to acquire an **exclusive lock** blocking until either the context is canceled or the lock can be acquired from the database.

### `Lock.RLock(ctx context.Context) error`

Attempts to acquire a **shared lock** and blocks until any exclusive locks are released or the context is canceled. Multiple sessions can acquire shared locks concurrently.

### `Lock.TryLock(ctx context.Context) (bool, error)`

Attempts to acquire an **exclusive lock** without waiting. Returns immediately with `true` if acquired, `false` otherwise.

### `Lock.TryRLock(ctx context.Context) (bool, error)`

Attempts to acquire a **shared lock** without waiting. Returns immediately with `true` if acquired, `false` otherwise. Multiple sessions can acquire shared locks concurrently.

### `Lock.Unlock(ctx context.Context) error`

Releases _one_ level of **exclusive lock** ownership. Must be called equal to the number of `Lock`/`TryLock` calls that succeed.

### `Lock.RUnlock(ctx context.Context) error`

Releases _one_ level of **shared lock** ownership. Must be called equal to the number of `RLock`/`TryRLock` calls that succeed.

### `Lock.Close() error`

Closes the underlying database connection (returning it to the pool) and ending the database session which has the effect of releasing all held locks (both exclusive and shared).