package topic

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	redis "gopkg.in/redis.v5"
)

var (
	randomString = func() string {
		var n uint64
		binary.Read(rand.Reader, binary.LittleEndian, &n)
		rndStr := strconv.FormatUint(n, 36)
		tsStr := strconv.FormatInt(time.Now().UnixNano(), 36)
		return tsStr + rndStr
	}
)

type pipelineImpl struct {
	*storeImpl
	tx *redis.Pipeline
}

func (p *pipelineImpl) Union(dest string, keys ...string) {
	p.tx.SUnionStore(dest, keys...)
	p.tx.Expire(dest, p.sessionTTL)
}

func (p *pipelineImpl) Inter(dest string, keys ...string) {
	p.tx.SInterStore(dest, keys...)
	p.tx.Expire(dest, p.sessionTTL)
}

func (p *pipelineImpl) Diff(dest string, keys ...string) {
	p.tx.SDiffStore(dest, keys...)
	p.tx.Expire(dest, p.sessionTTL)
}

func (p *pipelineImpl) Source(key string) string {
	return fmt.Sprintf(p.topicKeyFormat, key)
}

func (p *pipelineImpl) Session() string {
	key := randomString()
	return fmt.Sprintf(p.sessionKeyFormat, key)
}
