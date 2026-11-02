package pg_classify

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"apercu-cli/helper/pg_contract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notEffects are the codes a classifier builds its finding for directly, without an effect.
var notEffects = map[pg_contract.Code]bool{
	"R-AT-002":           true,
	"R-IX-ALLTABLESPACE": true,
	"R-MV-ALLTABLESPACE": true,
}

// Every rule code has to own its row: a code that falls back silently reaches every child at ACCESS EXCLUSIVE.
func TestClauseTableCoversEveryRule(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("rules_*.go")
	require.NoError(t, err)
	files = append(files, "alter_table.go")

	code := regexp.MustCompile(`"(R-[A-Z0-9]+-[A-Z0-9-]+)"`)
	seen := 0
	for _, file := range files {
		if filepath.Ext(file) != ".go" || regexp.MustCompile(`_test\.go$`).MatchString(file) {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range code.FindAllStringSubmatch(string(source), -1) {
			name := pg_contract.Code(match[1])
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
		lock:      pg_contract.LockAccessExclusive,
		below:     below{partitions: worked, partitionLock: pg_contract.LockShare},
		underOnly: below{partitions: refused},
		changes: []change{{
			from:      pg_contract.Version18,
			below:     below{partitions: worked, children: locked},
			underOnly: below{partitions: locked, children: locked},
		}},
	}

	reach, underOnly := row.reachAt(pg_contract.Version16)
	assert.Equal(t, below{partitions: worked, partitionLock: pg_contract.LockShare}, reach)
	assert.Equal(t, below{partitions: refused}, underOnly)

	reach, underOnly = row.reachAt(pg_contract.Version18)
	assert.Equal(t, below{partitions: worked, children: locked}, reach)
	assert.Equal(t, below{partitions: locked, children: locked}, underOnly)

	// An unknown version reads as the strongest of every version, and a refusal loses to any acceptance.
	reach, underOnly = row.reachAt(pg_contract.VersionUnknown)
	assert.Equal(t, below{partitions: worked, children: locked}, reach, "SHARE on 15-17 against the clause's own lock on 18")
	assert.Equal(t, below{partitions: locked, children: locked}, underOnly)
}

func TestClauseGate(t *testing.T) {
	t.Parallel()

	gated := clause{partitioned: pg_contract.AtLeast(pg_contract.Version17)}
	assert.False(t, gated.accepts(pg_contract.Version16))
	assert.True(t, gated.accepts(pg_contract.Version17))
	assert.True(t, gated.accepts(pg_contract.VersionUnknown), "an unknown production version reads as the newest")
	assert.True(t, clause{}.accepts(pg_contract.Version15), "the zero range accepts every version")
}
