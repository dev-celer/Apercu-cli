package seeding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDirectSeed_GetDuration(t *testing.T) {
	t.Parallel()

	t.Run("both times set", func(t *testing.T) {
		t.Parallel()
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 1, 0, 0, 3, 0, time.UTC)
		h := &DirectSeed{startTime: &start, endTime: &end}

		d := h.GetDuration()
		assert.NotNil(t, d)
		assert.Equal(t, 3*time.Second, *d)
	})

	t.Run("nil start time", func(t *testing.T) {
		t.Parallel()
		end := time.Now()
		h := &DirectSeed{endTime: &end}
		assert.Nil(t, h.GetDuration())
	})

	t.Run("nil end time", func(t *testing.T) {
		t.Parallel()
		start := time.Now()
		h := &DirectSeed{startTime: &start}
		assert.Nil(t, h.GetDuration())
	})

	t.Run("nil times", func(t *testing.T) {
		t.Parallel()
		h := &DirectSeed{}
		assert.Nil(t, h.GetDuration())
	})
}
