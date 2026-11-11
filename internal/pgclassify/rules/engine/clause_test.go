package engine

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notEffects are the codes a classifier builds its finding for directly, without an effect.
var notEffects = map[pgcontract.Code]bool{
	"R-AT-002":           true,
	"R-IX-ALLTABLESPACE": true,
	"R-MV-ALLTABLESPACE": true,
}

// Every rule code has to own its row: a code that falls back silently reaches every child at ACCESS EXCLUSIVE.
func TestClauseTableCoversEveryRule(t *testing.T) {
	t.Parallel()

	// The rules live one directory up, the registry among them.
	files, err := filepath.Glob(filepath.Join("..", "*.go"))
	require.NoError(t, err)

	code := regexp.MustCompile(`"(R-[A-Z0-9]+-[A-Z0-9-]+)"`)
	seen := 0
	for _, file := range files {
		if filepath.Ext(file) != ".go" || regexp.MustCompile(`_test\.go$`).MatchString(file) {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range code.FindAllStringSubmatch(string(source), -1) {
			name := pgcontract.Code(match[1])
			if notEffects[name] {
				continue
			}
			seen++
			_, ok := clauses[name]
			assert.Truef(t, ok, "%s (%s) has no row in the clause table", name, file)
		}
	}
	assert.Positive(t, seen)
}

func TestClauseReachByVersion(t *testing.T) {
	t.Parallel()

	row := clause{
		lock:      pgcontract.LockAccessExclusive,
		below:     Below{Partitions: Worked, PartitionLock: pgcontract.LockShare},
		underOnly: Below{Partitions: Refused},
		changes: []change{{
			from:      pgcontract.Version18,
			below:     Below{Partitions: Worked, Children: Locked},
			underOnly: Below{Partitions: Locked, Children: Locked},
		}},
	}

	reach, underOnly := row.reachAt(pgcontract.Version16)
	assert.Equal(t, Below{Partitions: Worked, PartitionLock: pgcontract.LockShare}, reach)
	assert.Equal(t, Below{Partitions: Refused}, underOnly)

	reach, underOnly = row.reachAt(pgcontract.Version18)
	assert.Equal(t, Below{Partitions: Worked, Children: Locked}, reach)
	assert.Equal(t, Below{Partitions: Locked, Children: Locked}, underOnly)

	// An unknown version reads as the strongest of every version, and a refusal loses to any acceptance.
	reach, underOnly = row.reachAt(pgcontract.VersionUnknown)
	assert.Equal(t, Below{Partitions: Worked, Children: Locked}, reach, "SHARE on 15-17 against the clause's own lock on 18")
	assert.Equal(t, Below{Partitions: Locked, Children: Locked}, underOnly)
}

func TestClauseGate(t *testing.T) {
	t.Parallel()

	gated := clause{partitioned: pgcontract.AtLeast(pgcontract.Version17)}
	assert.False(t, gated.accepts(pgcontract.Version16))
	assert.True(t, gated.accepts(pgcontract.Version17))
	assert.True(t, gated.accepts(pgcontract.VersionUnknown), "an unknown production version reads as the newest")
	assert.True(t, clause{}.accepts(pgcontract.Version15), "the zero range accepts every version")
}
