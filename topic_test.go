package topic

import (
	"testing"

	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/redis.v5"
)

var (
	testConfig = &Config{}
)

func TestSaveLastPushedAt(t *testing.T) {
	assert := assert.New(t)

	store := NewStore(testConfig)
	testDataOne := []uint64{}
	testDataTwo := []uint64{1, 2, 3, 4, 5}

	err := store.SaveLastPushedAt(testDataOne, time.Now())
	assert.Nil(err)

	err = store.SaveLastPushedAt(testDataTwo, time.Now())
	assert.NoError(err)

	// 他のテストケースに影響するため、ユーザID毎にハッシュにセットしたフィールドを削除する。
	err = resetLastPushedAtKey(testDataTwo)
	assert.NoError(err)
}

func TestGetLastPushedAt_ExpectsToGetEmptyData(t *testing.T) {
	assert := assert.New(t)
	store := NewStore(testConfig)

	actual, err := store.GetLastPushedAt([]uint64{})
	assert.Nil(actual)
	assert.Nil(err)
}

func TestGetLastPushedAt_ValidatesLastPushedAtAfterSetData(t *testing.T) {
	assert := assert.New(t)

	store := NewStore(testConfig)
	testDataOne := []uint64{1, 2, 3, 4, 5}
	testDataTwo := time.Date(2018, 1, 12, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2018, 1, 12, 0, 0, 0, 0, time.UTC).In(time.Local)

	_, err := sharedClient.TxPipelined(func(pipe *redis.Pipeline) error {
		for _, id := range testDataOne {
			pipe.HSet(fmt.Sprintf(defaultLastPushedAtKeyFormat, id), "like", testDataTwo.Unix())
		}
		return nil
	})
	assert.NoError(err)

	actual, err := store.GetLastPushedAt([]uint64{1, 2, 3, 4, 5})
	assert.Nil(err)
	if assert.Len(actual, 5) {
		assert.Equal(expected, actual[0])
		assert.Equal(expected, actual[1])
		assert.Equal(expected, actual[2])
		assert.Equal(expected, actual[3])
		assert.Equal(expected, actual[4])
	}

	// 他のテストケースに影響するため、ユーザID毎にハッシュにセットしたフィールドを削除する。
	err = resetLastPushedAtKey(testDataOne)
	assert.NoError(err)
}

func TestGetLastPushedAt_NotExistingKey(t *testing.T) {
	assert := assert.New(t)

	store := NewStore(testConfig)
	testDataOne := []uint64{6, 7, 8}
	expected := time.Time{}

	actual, err := store.GetLastPushedAt(testDataOne)
	assert.Nil(err)
	if assert.Len(actual, 3) {
		assert.Equal(expected, actual[0])
		assert.Equal(expected, actual[1])
		assert.Equal(expected, actual[2])
	}

	// 他のテストケースに影響するため、ユーザID毎にハッシュにセットしたフィールドを削除する。
	err = resetLastPushedAtKey(testDataOne)
	assert.NoError(err)
}

func resetLastPushedAtKey(target []uint64) (err error) {
	_, err = sharedClient.TxPipelined(func(pipe *redis.Pipeline) error {
		for _, id := range target {
			pipe.HDel(fmt.Sprintf(defaultLastPushedAtKeyFormat, id), "like")
		}
		return nil
	})

	return err
}
