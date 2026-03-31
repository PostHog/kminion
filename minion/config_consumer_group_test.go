package minion

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerGroupConfig_SetDefaults(t *testing.T) {
	var cfg ConsumerGroupConfig
	cfg.SetDefaults()

	assert.True(t, cfg.Enabled)
	assert.Equal(t, ConsumerGroupScrapeModeAdminAPI, cfg.ScrapeMode)
	assert.Equal(t, ConsumerGroupGranularityPartition, cfg.Granularity)
	assert.False(t, cfg.TimeLagEnabled)
	assert.Equal(t, 10, cfg.TimeLagFetchConcurrency)
	assert.Equal(t, int32(4096), cfg.TimeLagMaxFetchBytes)
}

func TestConsumerGroupConfig_Validate(t *testing.T) {
	validConfig := func() ConsumerGroupConfig {
		var cfg ConsumerGroupConfig
		cfg.SetDefaults()
		return cfg
	}

	tests := []struct {
		name    string
		cfg     ConsumerGroupConfig
		wantErr string
	}{
		{
			name: "valid defaults",
			cfg:  validConfig(),
		},
		{
			name: "valid with high concurrency",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.TimeLagFetchConcurrency = 200
				return cfg
			}(),
		},
		{
			name: "valid with custom max fetch bytes",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.TimeLagMaxFetchBytes = 1 << 20
				return cfg
			}(),
		},
		{
			name: "invalid scrape mode",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.ScrapeMode = "invalid"
				return cfg
			}(),
			wantErr: "invalid scrape mode",
		},
		{
			name: "invalid granularity",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.Granularity = "invalid"
				return cfg
			}(),
			wantErr: "invalid consumer group granularity",
		},
		{
			name: "zero concurrency",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.TimeLagFetchConcurrency = 0
				return cfg
			}(),
			wantErr: "timeLagFetchConcurrency must be at least 1",
		},
		{
			name: "negative concurrency",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.TimeLagFetchConcurrency = -5
				return cfg
			}(),
			wantErr: "timeLagFetchConcurrency must be at least 1",
		},
		{
			name: "zero max fetch bytes",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.TimeLagMaxFetchBytes = 0
				return cfg
			}(),
			wantErr: "timeLagMaxFetchBytes must be at least 1",
		},
		{
			name: "negative max fetch bytes",
			cfg: func() ConsumerGroupConfig {
				cfg := validConfig()
				cfg.TimeLagMaxFetchBytes = -1
				return cfg
			}(),
			wantErr: "timeLagMaxFetchBytes must be at least 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
