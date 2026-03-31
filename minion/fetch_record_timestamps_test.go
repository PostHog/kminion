package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordTimestamps_SetAndGet(t *testing.T) {
	rt := RecordTimestamps(make(map[string]map[int32]map[int64]int64))

	rt.Set("topic-a", 0, 100, 1000)
	rt.Set("topic-a", 0, 200, 2000)
	rt.Set("topic-a", 1, 100, 3000)
	rt.Set("topic-b", 0, 50, 4000)

	tests := []struct {
		name      string
		topic     string
		partition int32
		offset    int64
		wantTS    int64
		wantOK    bool
	}{
		{"existing entry", "topic-a", 0, 100, 1000, true},
		{"same partition different offset", "topic-a", 0, 200, 2000, true},
		{"same topic different partition", "topic-a", 1, 100, 3000, true},
		{"different topic", "topic-b", 0, 50, 4000, true},
		{"missing offset", "topic-a", 0, 999, 0, false},
		{"missing partition", "topic-a", 99, 100, 0, false},
		{"missing topic", "topic-c", 0, 100, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, ok := rt.Get(tt.topic, tt.partition, tt.offset)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantTS, ts)
		})
	}
}

func TestRecordTimestamps_OverwriteExisting(t *testing.T) {
	rt := RecordTimestamps(make(map[string]map[int32]map[int64]int64))

	rt.Set("topic", 0, 100, 1000)
	rt.Set("topic", 0, 100, 9999)

	ts, ok := rt.Get("topic", 0, 100)
	assert.True(t, ok)
	assert.Equal(t, int64(9999), ts)
}
