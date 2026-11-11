package prove_test

import (
	"testing"

	"apercu-cli/internal/pgclassify/classifytest"
	"apercu-cli/internal/pgclassify/validate"
	"apercu-cli/internal/pgcontract"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attachedOp is what R-AT-ATTACH reports doing to the partition it attaches.
func attachedOp(t *testing.T, analysis pgcontract.StatementAnalysis, partition string) pgcontract.OpKind {
	t.Helper()

	finding := classifytest.FindingOf(t, analysis, "R-AT-ATTACH")
	for _, target := range finding.Targets {
		if target.Relation.Name.Table == partition {
			return target.OpKind
		}
	}
	require.Failf(t, "missing target", "R-AT-ATTACH does not name %s", partition)
	return pgcontract.OpKindNone
}

func TestAttachBoundProof(t *testing.T) {
	t.Parallel()

	const bound = " FOR VALUES FROM (100) TO (200)"
	cases := []struct {
		name      string
		script    string
		partition string
		proven    bool
	}{
		{name: "a CHECK that says IS NOT NULL and the range", partition: "rd_proved", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "IS NOT NULL in a CHECK of its own", partition: "rd_split", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_split" + bound},
		{name: "a NOT NULL column", partition: "rd_notnull", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_notnull" + bound},
		{name: "a nullable key the CHECK leaves open", partition: "rd_bound",
			script: "ALTER TABLE readings ATTACH PARTITION rd_bound" + bound},
		{name: "a bound wider than the CHECK", partition: "rd_proved", proven: true,
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved FOR VALUES FROM (MINVALUE) TO (300)"},
		{name: "a bound narrower than the CHECK", partition: "rd_proved",
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved FOR VALUES FROM (100) TO (150)"},
		{name: "a NOT VALID CHECK", partition: "rd_notvalid",
			script: "ALTER TABLE readings ATTACH PARTITION rd_notvalid" + bound},
		{name: "a HASH bound", partition: "rd_proved",
			script: "ALTER TABLE readings_hash ATTACH PARTITION rd_proved FOR VALUES WITH (MODULUS 2, REMAINDER 0)"},
		{name: "a DEFAULT partition", partition: "rd_proved",
			script: "ALTER TABLE readings ATTACH PARTITION rd_proved DEFAULT"},
		{name: "a list CHECK spelling the NULL branch", partition: "rd_list_null", proven: true,
			script: "ALTER TABLE readings_list ATTACH PARTITION rd_list_null FOR VALUES IN (NULL, 1, 2, 3)"},
		{name: "a list CHECK without IS NOT NULL", partition: "rd_list",
			script: "ALTER TABLE readings_list ATTACH PARTITION rd_list FOR VALUES IN (1, 2)"},
		{name: "a CHECK added earlier in the migration", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at BETWEEN 100 AND 199); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a CHECK added NOT VALID, then validated", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at >= 100 AND at < 200) NOT VALID; " +
				"ALTER TABLE rd_plain VALIDATE CONSTRAINT b; ALTER TABLE rd_plain ALTER COLUMN at SET NOT NULL; " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a CHECK added NOT VALID and left so", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 100 AND at < 200) NOT VALID; " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a fractional constant on an integer key", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 99.5 AND at < 200); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an OR of ranges inside the bound", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND (at >= 100 AND at < 150 OR at >= 150 AND at < 200)); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an OR with an arm outside the bound", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND (at >= 100 AND at < 150 OR at >= 250)); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an OR with an arm the proof cannot read", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND (at >= 100 AND at < 150 OR abs(at) < 10)); " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an unnamed CHECK dropped under the name the server gave it", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain DROP CONSTRAINT rd_plain_at_check; ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an unnamed CHECK dropped under a suffix DDL nothing could read made the server pick", partition: "rd_plain",
			script: "DO $$ BEGIN END $$; ALTER TABLE rd_plain ADD CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain DROP CONSTRAINT rd_plain_at_check1; ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an added CHECK renamed", partition: "rd_plain", proven: true,
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain RENAME CONSTRAINT b TO c; ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "an added CHECK renamed, then dropped", partition: "rd_plain",
			script: "ALTER TABLE rd_plain ADD CONSTRAINT b CHECK (at IS NOT NULL AND at >= 100 AND at < 200); " +
				"ALTER TABLE rd_plain RENAME CONSTRAINT b TO c; ALTER TABLE rd_plain DROP CONSTRAINT c; " +
				"ALTER TABLE readings ATTACH PARTITION rd_plain" + bound},
		{name: "a snapshot CHECK renamed", partition: "rd_proved", proven: true,
			script: "ALTER TABLE rd_proved RENAME CONSTRAINT rd_proved_b TO c; ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "a snapshot CHECK renamed, then dropped", partition: "rd_proved",
			script: "ALTER TABLE rd_proved RENAME CONSTRAINT rd_proved_b TO c; ALTER TABLE rd_proved DROP CONSTRAINT c; " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "the key column renamed away and back", partition: "rd_proved", proven: true,
			script: "ALTER TABLE rd_proved RENAME COLUMN at TO old_at; ALTER TABLE rd_proved RENAME COLUMN old_at TO at; " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "the key column dropped and added back", partition: "rd_notnull",
			script: "ALTER TABLE rd_notnull DROP COLUMN at; ALTER TABLE rd_notnull ADD COLUMN at int; " +
				"ALTER TABLE readings ATTACH PARTITION rd_notnull" + bound},
		{name: "the key column renamed away and another added under its name", partition: "rd_proved",
			script: "ALTER TABLE rd_proved RENAME COLUMN at TO old_at; ALTER TABLE rd_proved ADD COLUMN at int; " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
		{name: "the table dropped and created again", partition: "rd_proved",
			script: "DROP TABLE rd_proved; CREATE TABLE rd_proved (id bigint NOT NULL, at int); " +
				"ALTER TABLE readings ATTACH PARTITION rd_proved" + bound},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			analyses := classifytest.Analyze(t, classifytest.NullableKeyCatalog(t), testCase.script)
			last := analyses[len(analyses)-1]
			want := pgcontract.OpKindScan
			if testCase.proven {
				want = pgcontract.OpKindMetadata
			}
			assert.Equal(t, want, attachedOp(t, last, testCase.partition))
			finding := classifytest.FindingOf(t, last, "R-AT-ATTACH")
			if testCase.proven {
				assert.Contains(t, finding.Message, "already imply the partition bound")
				assert.NotContains(t, classifytest.CodesOf(last), validate.NullableKeyCode, "a proven bound leaves nothing for V-03 to say")
			} else {
				assert.Contains(t, finding.Message, "is read in full")
			}
		})
	}
}
