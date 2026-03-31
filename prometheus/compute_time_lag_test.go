package prometheus

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cloudhut/kminion/v2/minion"
)

func TestComputeTimeLagSeconds(t *testing.T) {
	e := &Exporter{}
	nowMillis := int64(10_000)

	timestamps := minion.RecordTimestamps(make(map[string]map[int32]map[int64]int64))
	timestamps.Set("topic", 0, 50, 5_000)  // 5 seconds ago
	timestamps.Set("topic", 0, 90, 9_500)  // 0.5 seconds ago
	timestamps.Set("topic", 1, 10, 1_000)  // 9 seconds ago

	tests := []struct {
		name            string
		topic           string
		partition       int32
		committedOffset int64
		highWaterMark   int64
		want            float64
	}{
		{
			name:            "normal lag",
			topic:           "topic",
			partition:       0,
			committedOffset: 50,
			highWaterMark:   100,
			want:            5.0,
		},
		{
			name:            "small lag",
			topic:           "topic",
			partition:       0,
			committedOffset: 90,
			highWaterMark:   100,
			want:            0.5,
		},
		{
			name:            "different partition",
			topic:           "topic",
			partition:       1,
			committedOffset: 10,
			highWaterMark:   20,
			want:            9.0,
		},
		{
			name:            "caught up returns zero",
			topic:           "topic",
			partition:       0,
			committedOffset: 100,
			highWaterMark:   100,
			want:            0,
		},
		{
			name:            "negative committed offset returns -1",
			topic:           "topic",
			partition:       0,
			committedOffset: -1,
			highWaterMark:   100,
			want:            -1,
		},
		{
			name:            "missing timestamp returns -1",
			topic:           "topic",
			partition:       0,
			committedOffset: 999,
			highWaterMark:   1000,
			want:            -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := e.computeTimeLagSeconds(timestamps, tt.topic, tt.partition, tt.committedOffset, tt.highWaterMark, nowMillis)
			assert.InDelta(t, tt.want, got, 0.001)
		})
	}
}
