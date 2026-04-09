package migration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDockerHandler_GetDuration(t *testing.T) {
	t.Parallel()

	t.Run("both times set", func(t *testing.T) {
		t.Parallel()
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 1, 1, 0, 0, 5, 0, time.UTC)
		h := &DockerHandler{startTime: &start, endTime: &end}

		d := h.GetDuration()
		assert.NotNil(t, d)
		assert.Equal(t, 5*time.Second, *d)
	})

	t.Run("nil start time", func(t *testing.T) {
		t.Parallel()
		end := time.Now()
		h := &DockerHandler{endTime: &end}
		assert.Nil(t, h.GetDuration())
	})

	t.Run("nil end time", func(t *testing.T) {
		t.Parallel()
		start := time.Now()
		h := &DockerHandler{startTime: &start}
		assert.Nil(t, h.GetDuration())
	})

	t.Run("both nil", func(t *testing.T) {
		t.Parallel()
		h := &DockerHandler{}
		assert.Nil(t, h.GetDuration())
	})
}
