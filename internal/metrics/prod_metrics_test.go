package metrics

import (
	"apercu-cli/helper"
	metricshelper "apercu-cli/helper/metrics"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetActivityFromGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ops         *float64
		hotFloor    float64
		coldCeiling float64
		want        metricshelper.TableActivity
	}{
		{
			name:        "nil ops defaults to none",
			ops:         nil,
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityNone,
		},
		{
			name:        "above ceiling is hot",
			ops:         new(150.),
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityHot,
		},
		{
			name:        "exactly at ceiling is hot",
			ops:         new(100.),
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityHot,
		},
		{
			name:        "between floor and ceiling is warm",
			ops:         new(50.),
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityWarm,
		},
		{
			name:        "exactly at floor is warm",
			ops:         new(1.),
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityWarm,
		},
		{
			name:        "below floor is cold",
			ops:         new(0.5),
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityCold,
		},
		{
			name:        "zero is cold",
			ops:         new(0.),
			hotFloor:    DefaultHotFloorWriteActivity,
			coldCeiling: DefaultColdCeilingWriteActivity,
			want:        metricshelper.TableActivityCold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getActivityFromGuards(tt.ops, tt.hotFloor, tt.coldCeiling)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetActivityFromRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ops         *float64
		hotPct      float64
		warmPct     float64
		hotFloor    float64
		coldCeiling float64
		want        metricshelper.TableActivity
	}{
		{
			name:        "nil ops defaults to none",
			ops:         nil,
			hotPct:      100,
			warmPct:     10,
			hotFloor:    1,
			coldCeiling: 1000,
			want:        metricshelper.TableActivityNone,
		},
		{
			name:        "above hot percentile and above floor is hot",
			ops:         new(200.),
			hotPct:      100,
			warmPct:     10,
			hotFloor:    1,
			coldCeiling: 1000,
			want:        metricshelper.TableActivityHot,
		},
		{
			name:        "above hot percentile but below floor is not hot",
			ops:         new(50.),
			hotPct:      10,
			warmPct:     5,
			hotFloor:    100,
			coldCeiling: 1000,
			want:        metricshelper.TableActivityWarm,
		},
		{
			name:        "above warm percentile is warm",
			ops:         new(50.),
			hotPct:      100,
			warmPct:     10,
			hotFloor:    1,
			coldCeiling: 1000,
			want:        metricshelper.TableActivityWarm,
		},
		{
			name:        "below warm percentile but above cold ceiling is warm",
			ops:         new(200.),
			hotPct:      300,
			warmPct:     10,
			hotFloor:    1,
			coldCeiling: 100,
			want:        metricshelper.TableActivityWarm,
		},
		{
			name:        "below all thresholds is cold",
			ops:         new(0.5),
			hotPct:      100,
			warmPct:     10,
			hotFloor:    1,
			coldCeiling: 100,
			want:        metricshelper.TableActivityCold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getActivityFromRules(tt.ops, tt.hotPct, tt.warmPct, tt.hotFloor, tt.coldCeiling)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractPercentileFromDatapoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		datapoint []float64
		percent   float64
		want      float64
	}{
		{
			name:      "75th percentile interpolated",
			datapoint: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			percent:   0.75,
			want:      7.75,
		},
		{
			name:      "25th percentile interpolated",
			datapoint: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			percent:   0.25,
			want:      3.25,
		},
		{
			name:      "50th percentile interpolated",
			datapoint: []float64{10, 20, 30, 40},
			percent:   0.5,
			want:      25,
		},
		{
			name:      "75th percentile",
			datapoint: []float64{10, 20, 30, 40},
			percent:   0.75,
			want:      32.5,
		},
		{
			name:      "single element returns that element",
			datapoint: []float64{42},
			percent:   DefaultHotPercentile,
			want:      42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractPercentileFromDatapoint(tt.datapoint, tt.percent)
			assert.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

func TestInjectTableActivity_SmallSchemaUsesGuards(t *testing.T) {
	t.Parallel()

	metrics := metricshelper.DatabaseMetrics{
		TablesMetrics: map[helper.FullTableName]metricshelper.TableMetrics{
			{Schema: "public", Table: "hot"}:  {WritesPerSecond: new(150.), ScanPerSecond: new(150.)},
			{Schema: "public", Table: "warm"}: {WritesPerSecond: new(50.), ScanPerSecond: new(50.)},
			{Schema: "public", Table: "cold"}: {WritesPerSecond: new(0.5), ScanPerSecond: nil},
		},
	}

	injectTableActivity(&metrics)

	hot := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "hot"}]
	assert.Equal(t, metricshelper.TableActivityHot, hot.WriteActivity)
	assert.Equal(t, metricshelper.TableActivityHot, hot.ReadActivity)

	warm := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "warm"}]
	assert.Equal(t, metricshelper.TableActivityWarm, warm.WriteActivity)
	assert.Equal(t, metricshelper.TableActivityWarm, warm.ReadActivity)

	cold := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "cold"}]
	assert.Equal(t, metricshelper.TableActivityCold, cold.WriteActivity)
	// nil scan defaults to none
	assert.Equal(t, metricshelper.TableActivityNone, cold.ReadActivity)
}

func TestInjectTableActivity_LargeSchemaUsesPercentiles(t *testing.T) {
	t.Parallel()

	tables := map[helper.FullTableName]metricshelper.TableMetrics{}
	// 12 tables with read/write per second ramping from 1 to 12 so that
	// percentile analysis (>= MinTableCountForPercentile) kicks in.
	for i := 1; i <= 12; i++ {
		name := helper.FullTableName{Schema: "public", Table: string(rune('a' + i - 1))}
		v := float64(i)
		tables[name] = metricshelper.TableMetrics{WritesPerSecond: &v, ScanPerSecond: &v}
	}
	metrics := metricshelper.DatabaseMetrics{TablesMetrics: tables}

	injectTableActivity(&metrics)

	// Highest table should land on the hot side, lowest on the cold/warm side.
	top := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "l"}]
	assert.Equal(t, metricshelper.TableActivityHot, top.WriteActivity)
	assert.Equal(t, metricshelper.TableActivityHot, top.ReadActivity)

	bottom := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "a"}]
	assert.NotEqual(t, metricshelper.TableActivityHot, bottom.WriteActivity)
	assert.NotEqual(t, metricshelper.TableActivityHot, bottom.ReadActivity)
}

func TestInjectTableActivity_LargeSchemaAllBelowFloorIsCold(t *testing.T) {
	t.Parallel()

	tables := map[helper.FullTableName]metricshelper.TableMetrics{}
	// 10 tables (>= MinTableCountForPercentile so the percentile path is taken)
	// but all below the hot floor, so the filtered population is empty and the
	// classification must fall back to the guards and land on COLD.
	for i := 0; i < 10; i++ {
		name := helper.FullTableName{Schema: "public", Table: string(rune('a' + i))}
		v := DefaultHotFloorWriteActivity - 0.5
		tables[name] = metricshelper.TableMetrics{WritesPerSecond: &v, ScanPerSecond: &v}
	}
	metrics := metricshelper.DatabaseMetrics{TablesMetrics: tables}

	injectTableActivity(&metrics)

	for name, m := range metrics.TablesMetrics {
		assert.Equalf(t, metricshelper.TableActivityCold, m.WriteActivity, "table %s write activity", name.Table)
		assert.Equalf(t, metricshelper.TableActivityCold, m.ReadActivity, "table %s read activity", name.Table)
	}
}
