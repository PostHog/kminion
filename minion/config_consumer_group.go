package minion

import (
	"fmt"
)

const (
	ConsumerGroupScrapeModeOffsetsTopic string = "offsetsTopic"
	ConsumerGroupScrapeModeAdminAPI     string = "adminApi"

	ConsumerGroupGranularityTopic     string = "topic"
	ConsumerGroupGranularityPartition string = "partition"
)

type ConsumerGroupConfig struct {
	// Enabled specifies whether consumer groups shall be scraped and exported or not.
	Enabled bool `koanf:"enabled"`

	// Mode specifies whether we export consumer group offsets using the Admin API or by consuming the internal
	// __consumer_offsets topic.
	ScrapeMode string `koanf:"scrapeMode"`

	// Granularity can be per topic or per partition. If you want to reduce the number of exported metric series and
	// you aren't interested in per partition lags you could choose "topic" where all partition lags will be summed
	// and only topic lags will be exported.
	Granularity string `koanf:"granularity"`

	// TimeLagEnabled controls whether time-based lag metrics are computed. When enabled, KMinion will fetch the
	// record at each committed offset to determine its timestamp, which adds extra Kafka fetch requests per scrape.
	TimeLagEnabled bool `koanf:"timeLagEnabled"`

	// TimeLagFetchConcurrency controls the maximum number of concurrent Kafka Fetch requests when fetching record
	// timestamps for time-based lag. Higher values reduce total fetch time but increase peak network/memory usage.
	TimeLagFetchConcurrency int `koanf:"timeLagFetchConcurrency"`

	// TimeLagMaxFetchBytes controls the maximum number of bytes fetched per partition when retrieving record
	// timestamps. Only the first record batch is needed, so this can be kept small. The Kafka protocol guarantees
	// at least one complete record batch is returned even if it exceeds this limit.
	TimeLagMaxFetchBytes int32 `koanf:"timeLagMaxFetchBytes"`

	// AllowedGroups are regex strings of group ids that shall be exported
	AllowedGroupIDs []string `koanf:"allowedGroups"`

	// IgnoredGroups are regex strings of group ids that shall be ignored/skipped when exporting metrics. Ignored groups
	// take precedence over allowed groups.
	IgnoredGroupIDs []string `koanf:"ignoredGroups"`
}

func (c *ConsumerGroupConfig) SetDefaults() {
	c.Enabled = true
	c.ScrapeMode = ConsumerGroupScrapeModeAdminAPI
	c.Granularity = ConsumerGroupGranularityPartition
	c.TimeLagEnabled = false
	c.TimeLagFetchConcurrency = 10
	c.TimeLagMaxFetchBytes = 4096
	c.AllowedGroupIDs = []string{"/.*/"}
}

func (c *ConsumerGroupConfig) Validate() error {
	switch c.ScrapeMode {
	case ConsumerGroupScrapeModeOffsetsTopic, ConsumerGroupScrapeModeAdminAPI:
	default:
		return fmt.Errorf("invalid scrape mode '%v' specified. Valid modes are '%v' or '%v'",
			c.ScrapeMode,
			ConsumerGroupScrapeModeOffsetsTopic,
			ConsumerGroupScrapeModeAdminAPI)
	}

	switch c.Granularity {
	case ConsumerGroupGranularityTopic, ConsumerGroupGranularityPartition:
	default:
		return fmt.Errorf("invalid consumer group granularity '%v' specified. Valid modes are '%v' or '%v'",
			c.Granularity,
			ConsumerGroupGranularityTopic,
			ConsumerGroupGranularityPartition)
	}

	if c.TimeLagFetchConcurrency < 1 {
		return fmt.Errorf("timeLagFetchConcurrency must be at least 1, got %d", c.TimeLagFetchConcurrency)
	}
	if c.TimeLagMaxFetchBytes < 1 {
		return fmt.Errorf("timeLagMaxFetchBytes must be at least 1, got %d", c.TimeLagMaxFetchBytes)
	}

	// Check if all group strings are valid regex or literals
	for _, groupID := range c.AllowedGroupIDs {
		_, err := compileRegex(groupID)
		if err != nil {
			return fmt.Errorf("allowed group string '%v' is not valid regex", groupID)
		}
	}

	for _, groupID := range c.IgnoredGroupIDs {
		_, err := compileRegex(groupID)
		if err != nil {
			return fmt.Errorf("ignored group string '%v' is not valid regex", groupID)
		}
	}

	return nil
}
