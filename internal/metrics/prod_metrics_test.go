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
		name         string
		ops          *float64
		hotFloor     float64
		coldCeiling  float64
		want         metricshelper.TableActivity
		wantDecision metricshelper.ActivityDecision
	}{
		{
			name:         "nil ops defaults to none",
			ops:          nil,
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityNone,
			wantDecision: metricshelper.ActivityDecisionNone,
		},
		{
			name:         "above ceiling is hot",
			ops:          new(150.),
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityHot,
			wantDecision: metricshelper.ActivityDecisionLowCount,
		},
		{
			name:         "exactly at ceiling is hot",
			ops:          new(100.),
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityHot,
			wantDecision: metricshelper.ActivityDecisionLowCount,
		},
		{
			name:         "between floor and ceiling is warm",
			ops:          new(50.),
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityWarm,
			wantDecision: metricshelper.ActivityDecisionLowCount,
		},
		{
			name:         "exactly at floor is warm",
			ops:          new(1.),
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityWarm,
			wantDecision: metricshelper.ActivityDecisionLowCount,
		},
		{
			name:         "below floor is cold",
			ops:          new(0.5),
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityCold,
			wantDecision: metricshelper.ActivityDecisionLowCount,
		},
		{
			name:         "zero is cold",
			ops:          new(0.),
			hotFloor:     metricshelper.DefaultHotFloorWriteActivity,
			coldCeiling:  metricshelper.DefaultColdCeilingWriteActivity,
			want:         metricshelper.TableActivityCold,
			wantDecision: metricshelper.ActivityDecisionLowCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, gotDecision := getActivityFromGuards(tt.ops, tt.hotFloor, tt.coldCeiling)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantDecision, gotDecision)
		})
	}
}

func TestGetActivityFromRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		ops          *float64
		hotPct       float64
		warmPct      float64
		hotFloor     float64
		coldCeiling  float64
		want         metricshelper.TableActivity
		wantDecision metricshelper.ActivityDecision
	}{
		{
			name:         "nil ops defaults to none",
			ops:          nil,
			hotPct:       100,
			warmPct:      10,
			hotFloor:     1,
			coldCeiling:  1000,
			want:         metricshelper.TableActivityNone,
			wantDecision: metricshelper.ActivityDecisionNone,
		},
		{
			name:         "above hot percentile and above floor is hot",
			ops:          new(200.),
			hotPct:       100,
			warmPct:      10,
			hotFloor:     1,
			coldCeiling:  1000,
			want:         metricshelper.TableActivityHot,
			wantDecision: metricshelper.ActivityDecisionPercentile,
		},
		{
			name:         "above hot percentile but below floor is not hot",
			ops:          new(50.),
			hotPct:       10,
			warmPct:      5,
			hotFloor:     100,
			coldCeiling:  1000,
			want:         metricshelper.TableActivityWarm,
			wantDecision: metricshelper.ActivityDecisionFloor,
		},
		{
			name:         "above warm percentile is warm",
			ops:          new(50.),
			hotPct:       100,
			warmPct:      10,
			hotFloor:     1,
			coldCeiling:  1000,
			want:         metricshelper.TableActivityWarm,
			wantDecision: metricshelper.ActivityDecisionPercentile,
		},
		{
			name:         "below warm percentile but above cold ceiling is warm",
			ops:          new(200.),
			hotPct:       300,
			warmPct:      10,
			hotFloor:     1,
			coldCeiling:  100,
			want:         metricshelper.TableActivityWarm,
			wantDecision: metricshelper.ActivityDecisionCeiling,
		},
		{
			name:         "below all thresholds is cold",
			ops:          new(0.5),
			hotPct:       100,
			warmPct:      10,
			hotFloor:     1,
			coldCeiling:  100,
			want:         metricshelper.TableActivityCold,
			wantDecision: metricshelper.ActivityDecisionPercentile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, gotDecision := getActivityFromRules(tt.ops, tt.hotPct, tt.warmPct, tt.hotFloor, tt.coldCeiling)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantDecision, gotDecision)
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
			percent:   metricshelper.DefaultHotPercentile,
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
	assert.Equal(t, metricshelper.ActivityDecisionLowCount, hot.WriteDecision)
	assert.Equal(t, metricshelper.ActivityDecisionLowCount, hot.ReadDecision)

	warm := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "warm"}]
	assert.Equal(t, metricshelper.TableActivityWarm, warm.WriteActivity)
	assert.Equal(t, metricshelper.TableActivityWarm, warm.ReadActivity)
	assert.Equal(t, metricshelper.ActivityDecisionLowCount, warm.WriteDecision)
	assert.Equal(t, metricshelper.ActivityDecisionLowCount, warm.ReadDecision)

	cold := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "cold"}]
	assert.Equal(t, metricshelper.TableActivityCold, cold.WriteActivity)
	// nil scan defaults to none
	assert.Equal(t, metricshelper.TableActivityNone, cold.ReadActivity)
	assert.Equal(t, metricshelper.ActivityDecisionLowCount, cold.WriteDecision)
	assert.Equal(t, metricshelper.ActivityDecisionNone, cold.ReadDecision)
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
	assert.Equal(t, metricshelper.ActivityDecisionPercentile, top.WriteDecision)
	assert.Equal(t, metricshelper.ActivityDecisionPercentile, top.WriteDecision)

	bottom := metrics.TablesMetrics[helper.FullTableName{Schema: "public", Table: "a"}]
	assert.Equal(t, metricshelper.TableActivityCold, bottom.WriteActivity)
	assert.Equal(t, metricshelper.TableActivityCold, bottom.ReadActivity)
	assert.Equal(t, metricshelper.ActivityDecisionPercentile, bottom.WriteDecision)
	assert.Equal(t, metricshelper.ActivityDecisionPercentile, bottom.WriteDecision)
}

func TestInjectTableActivity_LargeSchemaAllBelowFloorIsCold(t *testing.T) {
	t.Parallel()

	tables := map[helper.FullTableName]metricshelper.TableMetrics{}
	// 10 tables (>= MinTableCountForPercentile so the percentile path is taken)
	// but all below the hot floor, so the filtered population is empty and the
	// classification must fall back to the guards and land on COLD.
	for i := 0; i < 10; i++ {
		name := helper.FullTableName{Schema: "public", Table: string(rune('a' + i))}
		v := metricshelper.DefaultHotFloorWriteActivity - 0.5
		tables[name] = metricshelper.TableMetrics{WritesPerSecond: &v, ScanPerSecond: &v}
	}
	metrics := metricshelper.DatabaseMetrics{TablesMetrics: tables}

	injectTableActivity(&metrics)

	for name, m := range metrics.TablesMetrics {
		assert.Equalf(t, metricshelper.TableActivityCold, m.WriteActivity, "table %s write activity", name.Table)
		assert.Equal(t, metricshelper.ActivityDecisionLowCount, m.WriteDecision)
		assert.Equalf(t, metricshelper.TableActivityCold, m.ReadActivity, "table %s read activity", name.Table)
		assert.Equal(t, metricshelper.ActivityDecisionLowCount, m.ReadDecision)
	}
}
