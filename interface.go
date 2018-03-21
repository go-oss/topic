package topic

import (
	"context"
	"time"
)

// Store represents topic store.
type Store interface {
	Subscribe(topic Type, userIDs ...uint64) error
	Unsubscribe(topic Type, userIDs ...uint64) error
	ExpireAt(topic Type, expireAt time.Time) error
	NewSession(cond Expr) *sessionImpl
	SaveLastPushedAt(userIDs []uint64, lastPushDate time.Time) error
	GetLastPushedAt(userIDs []uint64) (res []time.Time, err error)
}

var _ Store = (*storeImpl)(nil)

// Pipeline represents set operation.
type Pipeline interface {
	Union(dest string, keys ...string)
	Inter(dest string, keys ...string)
	Diff(dest string, keys ...string)
	Source(key string) string
	Session() string
}

var _ Pipeline = (*pipelineImpl)(nil)

// Expr represents topic condition.
type Expr interface {
	And(targets ...Expr) Expr
	Or(targets ...Expr) Expr
	Not(targets ...Expr) Expr
	Exec(Pipeline) (key string)
}

var _ Expr = (*source)(nil)
var _ Expr = (*condAnd)(nil)
var _ Expr = (*condOr)(nil)
var _ Expr = (*condNot)(nil)

// Session represents scan results operation.
type Session interface {
	Scan(ctx context.Context, maxScanCount int64, bulk Bulk) (totalSuccess int64, err error)
}

var _ Session = (*sessionImpl)(nil)
