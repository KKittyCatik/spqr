package provider

import (
	"testing"

	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/stretchr/testify/assert"
)

// TestFitsOnShard tests the fitsOnShard method
func TestFitsOnShard(t *testing.T) {
	tests := []struct {
		name           string
		threshold      []float64
		krMetrics      []float64
		keyCountToMove int
		krKeyCount     int
		shardMetrics   *ShardMetrics
		expectedFit    bool
	}{
		{
			name:           "fits on shard with low load",
			threshold:      []float64{100.0, 1000.0},
			krMetrics:      []float64{10.0, 100.0},
			keyCountToMove: 5,
			krKeyCount:     10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{20.0, 200.0},
			},
			expectedFit: true,
		},
		{
			name:           "does not fit on shard - exceeds cpu threshold",
			threshold:      []float64{100.0, 1000.0},
			krMetrics:      []float64{50.0, 100.0},
			keyCountToMove: 10,
			krKeyCount:     10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{60.0, 200.0},
			},
			expectedFit: false,
		},
		{
			name:           "does not fit on shard - exceeds space threshold",
			threshold:      []float64{100.0, 1000.0},
			krMetrics:      []float64{10.0, 500.0},
			keyCountToMove: 10,
			krKeyCount:     10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{20.0, 600.0},
			},
			expectedFit: false,
		},
		{
			name:           "fits exactly on threshold",
			threshold:      []float64{100.0, 1000.0},
			krMetrics:      []float64{10.0, 100.0},
			keyCountToMove: 10,
			krKeyCount:     10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{0.0, 0.0},
			},
			expectedFit: true, // 100 < 100 is false, so threshold is not exceeded, it fits
		},
		{
			name:           "fits just below threshold",
			threshold:      []float64{100.0, 1000.0},
			krMetrics:      []float64{10.0, 100.0},
			keyCountToMove: 9,
			krKeyCount:     10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{10.0, 100.0},
			},
			expectedFit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			b := &BalancerImpl{
				threshold: tt.threshold,
			}
			result := b.fitsOnShard(tt.krMetrics, tt.keyCountToMove, tt.krKeyCount, tt.shardMetrics)
			assert.Equal(tt.expectedFit, result)
		})
	}
}

// TestMaxFitOnShard tests the maxFitOnShard method
func TestMaxFitOnShard(t *testing.T) {
	tests := []struct {
		name          string
		threshold     []float64
		krMetrics     []float64
		krKeyCount    int64
		shardMetrics  *ShardMetrics
		expectedCount int
	}{
		{
			name:       "calculate max fit with room for keys",
			threshold:  []float64{100.0, 1000.0},
			krMetrics:  []float64{10.0, 100.0},
			krKeyCount: 10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{20.0, 200.0},
			},
			expectedCount: 64, // cpu: 0.8*((100-20)/(10/10)) = 64, space: 0.8*((1000-200)/(100/10)) = 64, returns max across all metrics
		},
		{
			name:       "no room for keys - shard at capacity",
			threshold:  []float64{100.0, 1000.0},
			krMetrics:  []float64{10.0, 100.0},
			krKeyCount: 10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{100.0, 1000.0},
			},
			expectedCount: 0,
		},
		{
			name:       "space is limiting factor",
			threshold:  []float64{1000.0, 100.0},
			krMetrics:  []float64{10.0, 10.0},
			krKeyCount: 10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{900.0, 50.0},
			},
			expectedCount: 80, // cpu: 0.8*((1000-900)/(10/10)) = 80, space: 0.8*((100-50)/(10/10)) = 40, returns max across all metrics
		},
		{
			name:       "multiple metrics - returns max",
			threshold:  []float64{200.0, 500.0},
			krMetrics:  []float64{20.0, 25.0},
			krKeyCount: 10,
			shardMetrics: &ShardMetrics{
				MetricsTotal: []float64{100.0, 100.0},
			},
			expectedCount: 128, // cpu: 0.8*((200-100)/(20/10)) = 40, space: 0.8*((500-100)/(25/10)) = 128, returns max across all metrics
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			b := &BalancerImpl{
				threshold: tt.threshold,
			}
			result := b.maxFitOnShard(tt.krMetrics, tt.krKeyCount, tt.shardMetrics)
			assert.Equal(tt.expectedCount, result)
		})
	}
}

// TestMoveMaxPossible tests the moveMaxPossible method
func TestMoveMaxPossible(t *testing.T) {
	tests := []struct {
		name                string
		threshold           []float64
		shardMetrics        []*ShardMetrics
		shardIdToMetrics    map[string]*ShardMetrics
		krId                string
		krShardId           string
		expectedShardId     string
		expectedMaxKeyCount int
	}{
		{
			name:      "select shard with most capacity",
			threshold: []float64{100.0, 1000.0},
			shardMetrics: []*ShardMetrics{
				{
					ShardId:      "shard1",
					MetricsTotal: []float64{80.0, 800.0},
				},
				{
					ShardId:      "shard2",
					MetricsTotal: []float64{20.0, 200.0},
				},
				{
					ShardId:      "shard3",
					MetricsTotal: []float64{50.0, 500.0},
				},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"source": {
					ShardId: "source",
					MetricsKR: map[string][]float64{
						"kr1": {10.0, 100.0},
					},
					KeyCountKR: map[string]int64{
						"kr1": 10,
					},
				},
				"shard1": {
					ShardId:      "shard1",
					MetricsTotal: []float64{80.0, 800.0},
				},
				"shard2": {
					ShardId:      "shard2",
					MetricsTotal: []float64{20.0, 200.0},
				},
				"shard3": {
					ShardId:      "shard3",
					MetricsTotal: []float64{50.0, 500.0},
				},
			},
			krId:                "kr1",
			krShardId:           "source",
			expectedShardId:     "shard2",
			expectedMaxKeyCount: 64,
		},
		{
			name:      "no shards can fit any keys",
			threshold: []float64{100.0, 1000.0},
			shardMetrics: []*ShardMetrics{
				{
					ShardId:      "shard1",
					MetricsTotal: []float64{100.0, 1000.0},
				},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"source": {
					ShardId: "source",
					MetricsKR: map[string][]float64{
						"kr1": {10.0, 100.0},
					},
					KeyCountKR: map[string]int64{
						"kr1": 10,
					},
				},
				"shard1": {
					ShardId:      "shard1",
					MetricsTotal: []float64{100.0, 1000.0},
				},
			},
			krId:                "kr1",
			krShardId:           "source",
			expectedShardId:     "shard1",
			expectedMaxKeyCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			b := &BalancerImpl{
				threshold: tt.threshold,
			}
			shardId, maxKeyCount := b.moveMaxPossible(tt.shardMetrics, tt.shardIdToMetrics, tt.krId, tt.krShardId)
			assert.Equal(tt.expectedShardId, shardId)
			assert.Equal(tt.expectedMaxKeyCount, maxKeyCount)
		})
	}
}

// TestGetShardToMoveTo tests the getShardToMoveTo method
func TestGetShardToMoveTo(t *testing.T) {
	tests := []struct {
		name             string
		threshold        []float64
		shardMetrics     []*ShardMetrics
		shardIdToMetrics map[string]*ShardMetrics
		dsToKeyRanges    map[string][]*kr.KeyRange
		dsToKrIdx        map[string]map[string]int
		krToDs           map[string]string
		krId             string
		krShardId        string
		keyCountToMove   int
		expectedShardId  string
		expectedSuccess  bool
	}{
		{
			name:      "move to adjacent shard - only one adjacent shard fits",
			threshold: []float64{100.0, 1000.0},
			shardMetrics: []*ShardMetrics{
				{
					ShardId:      "shard1",
					MetricsTotal: []float64{96.0, 951.0},
				},
				{
					ShardId:      "shard2",
					MetricsTotal: []float64{20.0, 200.0},
				},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"source": {
					ShardId: "source",
					MetricsKR: map[string][]float64{
						"kr1": {10.0, 100.0},
					},
					KeyCountKR: map[string]int64{
						"kr1": 10,
					},
				},
				"shard1": {
					ShardId:      "shard1",
					MetricsTotal: []float64{96.0, 951.0},
				},
				"shard2": {
					ShardId:      "shard2",
					MetricsTotal: []float64{20.0, 200.0},
				},
			},
			dsToKeyRanges: map[string][]*kr.KeyRange{
				"ds1": {
					{ID: "kr0", ShardID: "shard1"},
					{ID: "kr1", ShardID: "source"},
					{ID: "kr2", ShardID: "shard2"},
				},
			},
			dsToKrIdx: map[string]map[string]int{
				"ds1": {
					"kr0": 0,
					"kr1": 1,
					"kr2": 2,
				},
			},
			krToDs: map[string]string{
				"kr0": "ds1",
				"kr1": "ds1",
				"kr2": "ds1",
			},
			krId:            "kr1",
			krShardId:       "source",
			keyCountToMove:  5,
			expectedShardId: "shard2",
			expectedSuccess: true,
		},
		{
			name:      "move to non-adjacent shard when both adjacent shards are full",
			threshold: []float64{100.0, 1000.0},
			shardMetrics: []*ShardMetrics{
				{
					ShardId:      "shard1",
					MetricsTotal: []float64{96.0, 951.0},
				},
				{
					ShardId:      "shard2",
					MetricsTotal: []float64{20.0, 200.0},
				},
				{
					ShardId:      "shard3",
					MetricsTotal: []float64{96.0, 951.0},
				},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"source": {
					ShardId: "source",
					MetricsKR: map[string][]float64{
						"kr1": {10.0, 100.0},
					},
					KeyCountKR: map[string]int64{
						"kr1": 10,
					},
				},
				"shard1": {
					ShardId:      "shard1",
					MetricsTotal: []float64{96.0, 951.0},
				},
				"shard2": {
					ShardId:      "shard2",
					MetricsTotal: []float64{20.0, 200.0},
				},
				"shard3": {
					ShardId:      "shard3",
					MetricsTotal: []float64{96.0, 951.0},
				},
			},
			dsToKeyRanges: map[string][]*kr.KeyRange{
				"ds1": {
					{ID: "kr0", ShardID: "shard1"},
					{ID: "kr1", ShardID: "source"},
					{ID: "kr2", ShardID: "shard3"},
				},
			},
			dsToKrIdx: map[string]map[string]int{
				"ds1": {
					"kr0": 0,
					"kr1": 1,
					"kr2": 2,
				},
			},
			krToDs: map[string]string{
				"kr0": "ds1",
				"kr1": "ds1",
				"kr2": "ds1",
			},
			krId:            "kr1",
			krShardId:       "source",
			keyCountToMove:  5,
			expectedShardId: "shard2",
			expectedSuccess: true,
		},
		{
			name:      "cannot move keys - no shard has capacity",
			threshold: []float64{100.0, 1000.0},
			shardMetrics: []*ShardMetrics{
				{
					ShardId:      "shard1",
					MetricsTotal: []float64{95.0, 950.0},
				},
			},
			shardIdToMetrics: map[string]*ShardMetrics{
				"source": {
					ShardId: "source",
					MetricsKR: map[string][]float64{
						"kr1": {10.0, 100.0},
					},
					KeyCountKR: map[string]int64{
						"kr1": 10,
					},
				},
				"shard1": {
					ShardId:      "shard1",
					MetricsTotal: []float64{95.0, 950.0},
				},
			},
			dsToKeyRanges: map[string][]*kr.KeyRange{
				"ds1": {
					{ID: "kr0", ShardID: "shard1"},
					{ID: "kr1", ShardID: "source"},
				},
			},
			dsToKrIdx: map[string]map[string]int{
				"ds1": {
					"kr0": 0,
					"kr1": 1,
				},
			},
			krToDs: map[string]string{
				"kr0": "ds1",
				"kr1": "ds1",
			},
			krId:            "kr1",
			krShardId:       "source",
			keyCountToMove:  10,
			expectedShardId: "",
			expectedSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			b := &BalancerImpl{
				threshold:     tt.threshold,
				dsToKeyRanges: tt.dsToKeyRanges,
				dsToKrIdx:     tt.dsToKrIdx,
				krToDs:        tt.krToDs,
			}
			shardId, success := b.getShardToMoveTo(tt.shardMetrics, tt.shardIdToMetrics, tt.krId, tt.krShardId, tt.keyCountToMove)
			assert.Equal(tt.expectedSuccess, success)
			if tt.expectedSuccess {
				assert.Equal(tt.expectedShardId, shardId)
			}
		})
	}
}
