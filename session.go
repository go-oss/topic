package topic

import (
	"context"
	"log"
	"strconv"

	"github.com/pkg/errors"
)

// sessionImpl implement Session interface.
type sessionImpl struct {
	*storeImpl
	expr Expr
}

// NewSession returns topic session.
func (s *storeImpl) NewSession(cond Expr) *sessionImpl {
	return &sessionImpl{s, cond}
}

// Scan exec topic condition.
func (s *sessionImpl) Scan(ctx context.Context, maxScanCount int64, bulk Bulk) (totalSuccess int64, err error) {
	tx := sharedClient.TxPipeline()
	key := s.expr.Exec(&pipelineImpl{s.storeImpl, tx})
	scanCmd := tx.SScan(key, 0, "", maxScanCount)
	cmds, err := tx.Exec()
	if err != nil {
		err = errors.Wrap(txError(cmds), err.Error())
		return totalSuccess, err
	}

	userIDs := make([]uint64, 0, maxScanCount)
	var cursor uint64 = 1
	for cursor > 0 {
		select {
		case <-ctx.Done(): // timeout or cancel
			return
		default:
			var res []string
			res, cursor, err = scanCmd.Result()
			if err != nil {
				return totalSuccess, errors.WithStack(err)
			}

			for _, uid := range res {
				userID, err := strconv.ParseUint(uid, 10, 64)
				if err != nil {
					log.Printf("Failed to strconv.ParseUint: %+v", err)
					continue
				}

				userIDs = append(userIDs, userID)

				if len(userIDs) == int(maxScanCount) {
					success, err := bulk(userIDs)
					if err != nil {
						return totalSuccess, err
					}
					totalSuccess += success
					userIDs = userIDs[:0]
				}
			}

			if cursor > 0 {
				scanCmd = sharedClient.SScan(key, cursor, "", maxScanCount)
			}
		}
	}

	if len(userIDs) > 0 {
		success, err := bulk(userIDs)
		if err != nil {
			return totalSuccess, err
		}
		totalSuccess += success
	}

	return totalSuccess, nil
}
