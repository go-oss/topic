package topic

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/pkg/errors"
	redis "gopkg.in/redis.v5"
)

const (
	defaultTopicKeyFormat        = "topic:%s"
	defaultLastPushedAtKeyFormat = "users:%d:push"
	defaultSessionKeyFormat      = "sess:%s"
	defaultSessionTTL            = time.Minute * 5
)

var (
	sharedClient *redis.Client
)

// Type represents topic type.
type Type interface {
	String() string
}

// storeImpl implements Store interface.
type storeImpl struct {
	topicKeyFormat        string
	lastPushedAtKeyFormat string
	sessionKeyFormat      string
	sessionTTL            time.Duration
}

// Config represents redis client options.
type Config struct {
	Redis                 *redis.Options
	TopicKeyFormat        string
	LastPushedAtKeyFormat string
	SessionKeyFormat      string
	SessionTTL            time.Duration
}

// NewStore creates a new store
func NewStore(config *Config) Store {
	if config.Redis == nil {
		config.Redis = &redis.Options{
			Addr: "localhost:6379",
		}
	}

	connect(config.Redis)
	store := &storeImpl{
		topicKeyFormat:        defaultTopicKeyFormat,
		lastPushedAtKeyFormat: defaultLastPushedAtKeyFormat,
		sessionKeyFormat:      defaultSessionKeyFormat,
		sessionTTL:            defaultSessionTTL,
	}

	if config.TopicKeyFormat != "" {
		store.topicKeyFormat = config.TopicKeyFormat
	}
	if config.LastPushedAtKeyFormat != "" {
		store.lastPushedAtKeyFormat = config.LastPushedAtKeyFormat
	}
	if config.SessionKeyFormat != "" {
		store.sessionKeyFormat = config.SessionKeyFormat
	}
	if config.SessionTTL > 0 {
		store.sessionTTL = config.SessionTTL
	}

	return store
}

func connect(options *redis.Options) {
	if sharedClient == nil {
		// create redis client
		client := redis.NewClient(options)

		// check connection
		if _, err := client.Ping().Result(); err != nil {
			log.Print("failed to connect to redis client", err)
		}

		sharedClient = client
	}
}

func (s *storeImpl) Subscribe(topic Type, userIDs ...uint64) error {
	if len(userIDs) == 0 {
		return nil
	}

	key := fmt.Sprintf(s.topicKeyFormat, topic)

	uids := make([]interface{}, len(userIDs))
	for i, userID := range userIDs {
		uids[i] = userID
	}

	err := sharedClient.SAdd(key, uids...).Err()
	return errors.WithStack(err)
}

func (s *storeImpl) Unsubscribe(topic Type, userIDs ...uint64) error {
	if len(userIDs) == 0 {
		return nil
	}

	key := fmt.Sprintf(s.topicKeyFormat, topic)

	uids := make([]interface{}, len(userIDs))
	for i, userID := range userIDs {
		uids[i] = userID
	}

	err := sharedClient.SRem(key, uids...).Err()
	return errors.WithStack(err)
}

func (s *storeImpl) ExpireAt(topic Type, expireAt time.Time) error {
	key := fmt.Sprintf(s.topicKeyFormat, topic)
	err := sharedClient.ExpireAt(key, expireAt).Err()
	return errors.WithStack(err)
}

// SaveLastPushedAt saves last pushed time to hash.
func (s *storeImpl) SaveLastPushedAt(userIDs []uint64, lastPushDate time.Time) error {
	if len(userIDs) == 0 {
		return nil
	}

	cmds, err := sharedClient.TxPipelined(func(pipe *redis.Pipeline) error {
		for _, id := range userIDs {
			pipe.HSet(fmt.Sprintf(s.lastPushedAtKeyFormat, id), "like", lastPushDate.Unix())
		}
		return nil
	})
	if err != nil {
		err = txError(cmds)
	}

	return errors.WithStack(err)
}

// GetLastPushedAt get last pushed time to hash.
func (s *storeImpl) GetLastPushedAt(userIDs []uint64) (res []time.Time, err error) {
	if len(userIDs) == 0 {
		return nil, nil
	}

	res = make([]time.Time, 0, len(userIDs))

	outputs := make([]*redis.StringCmd, 0, len(userIDs))
	cmds, err := sharedClient.TxPipelined(func(pipe *redis.Pipeline) error {
		for _, id := range userIDs {
			outputs = append(outputs, pipe.HGet(fmt.Sprintf(s.lastPushedAtKeyFormat, id), "like"))
		}
		return nil
	})
	if err != nil {
		for _, v := range cmds {
			if v.Err() != nil && v.Err() != redis.Nil {
				return nil, txError(cmds)
			}
		}
	}

	for _, cmd := range outputs {
		unixTimeStr, err := cmd.Result()

		if unixTimeStr == "" || err != nil {
			res = append(res, time.Time{})
		} else {
			unixTime, err := strconv.ParseInt(unixTimeStr, 10, 64)
			if err != nil {
				res = append(res, time.Time{})
			}

			lastPushedAt := time.Unix(unixTime, 0)
			res = append(res, lastPushedAt.UTC())
		}
	}

	return res, nil
}

// Bulk represents scan callback method.
type Bulk func(userIDs []uint64) (int64, error)
