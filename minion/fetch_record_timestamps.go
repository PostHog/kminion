package minion

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// TopicPartitionOffset identifies a specific offset in a topic-partition.
type TopicPartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
}

// RecordTimestamps maps topic -> partition -> offset -> timestamp (milliseconds since epoch).
type RecordTimestamps map[string]map[int32]map[int64]int64

func (rt RecordTimestamps) Set(topic string, partition int32, offset int64, timestamp int64) {
	if _, ok := rt[topic]; !ok {
		rt[topic] = make(map[int32]map[int64]int64)
	}
	if _, ok := rt[topic][partition]; !ok {
		rt[topic][partition] = make(map[int64]int64)
	}
	rt[topic][partition][offset] = timestamp
}

func (rt RecordTimestamps) Get(topic string, partition int32, offset int64) (int64, bool) {
	if tp, ok := rt[topic]; ok {
		if offsets, ok := tp[partition]; ok {
			ts, ok := offsets[offset]
			return ts, ok
		}
	}
	return 0, false
}

// FetchRecordTimestampsCached fetches the record timestamp for each given offset, using
// per-scrape caching and singleflight deduplication.
func (s *Service) FetchRecordTimestampsCached(ctx context.Context, offsets []TopicPartitionOffset) (RecordTimestamps, error) {
	reqId := ctx.Value("requestId").(string)
	key := "record-timestamps-" + reqId

	if cachedRes, exists := s.getCachedItem(key); exists {
		return cachedRes.(RecordTimestamps), nil
	}

	res, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		timestamps, err := s.fetchRecordTimestamps(ctx, offsets)
		if err != nil {
			return nil, err
		}
		s.setCachedItem(key, timestamps, 120*time.Second)
		return timestamps, nil
	})
	if err != nil {
		return nil, err
	}

	return res.(RecordTimestamps), nil
}

// fetchRecordTimestamps fetches timestamps for a deduplicated set of offsets concurrently.
func (s *Service) fetchRecordTimestamps(ctx context.Context, offsets []TopicPartitionOffset) (RecordTimestamps, error) {
	// Deduplicate by (topic, partition, offset)
	type tpoKey struct {
		Topic     string
		Partition int32
		Offset    int64
	}
	seen := make(map[tpoKey]struct{})
	var unique []TopicPartitionOffset
	for _, o := range offsets {
		k := tpoKey{o.Topic, o.Partition, o.Offset}
		if _, exists := seen[k]; !exists {
			seen[k] = struct{}{}
			unique = append(unique, o)
		}
	}

	results := RecordTimestamps(make(map[string]map[int32]map[int64]int64))
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(10)

	for _, tpo := range unique {
		tpo := tpo
		g.Go(func() error {
			ts, err := s.fetchSingleRecordTimestamp(gCtx, tpo.Topic, tpo.Partition, tpo.Offset)
			if err != nil {
				s.logger.Debug("failed to fetch record timestamp for time-based lag",
					zap.String("topic", tpo.Topic),
					zap.Int32("partition", tpo.Partition),
					zap.Int64("offset", tpo.Offset),
					zap.Error(err))
				return nil // Don't fail the whole batch
			}
			mu.Lock()
			results.Set(tpo.Topic, tpo.Partition, tpo.Offset, ts)
			mu.Unlock()
			return nil
		})
	}

	_ = g.Wait()
	return results, nil
}

// fetchSingleRecordTimestamp fetches a single record at the given offset and returns its timestamp.
func (s *Service) fetchSingleRecordTimestamp(ctx context.Context, topic string, partition int32, offset int64) (int64, error) {
	req := kmsg.NewFetchRequest()
	req.MaxWaitMillis = 5000
	req.MinBytes = 1
	req.MaxBytes = 1 << 20 // 1MB

	reqTopic := kmsg.NewFetchRequestTopic()
	reqTopic.Topic = topic

	reqPartition := kmsg.NewFetchRequestTopicPartition()
	reqPartition.Partition = partition
	reqPartition.FetchOffset = offset
	reqPartition.PartitionMaxBytes = 1 << 20 // 1MB

	reqTopic.Partitions = []kmsg.FetchRequestTopicPartition{reqPartition}
	req.Topics = []kmsg.FetchRequestTopic{reqTopic}

	resp, err := req.RequestWith(ctx, s.client)
	if err != nil {
		return 0, fmt.Errorf("fetch request failed: %w", err)
	}

	if len(resp.Topics) == 0 {
		return 0, fmt.Errorf("no topics in fetch response")
	}
	if len(resp.Topics[0].Partitions) == 0 {
		return 0, fmt.Errorf("no partitions in fetch response")
	}

	p := resp.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
		return 0, fmt.Errorf("partition error: %w", err)
	}

	rawBatches := p.RecordBatches
	if len(rawBatches) == 0 {
		return 0, fmt.Errorf("no record batches returned")
	}

	// Parse the first record batch to get FirstTimestamp.
	// This is the timestamp of the first record in the batch containing (or starting at) the requested offset.
	// Within a single batch, records are written together, so the time spread is typically very small.
	var batch kmsg.RecordBatch
	if err := batch.ReadFrom(rawBatches); err != nil {
		return 0, fmt.Errorf("failed to decode record batch: %w", err)
	}

	if batch.Magic != 2 {
		return 0, fmt.Errorf("unsupported record batch magic version: %d", batch.Magic)
	}

	return batch.FirstTimestamp, nil
}
