package pg_classify

import (
	"fmt"

	"apercu-cli/helper/pg_contract"
	"apercu-cli/helper/pg_parse"
)

// gateCode is V-05, the statement is not supported in this server version.
const gateCode pg_contract.Code = "V-05"

// versionGate checks that a statement is compatible with the production version, and emit errors if it isn't the case.
func versionGate(version pg_contract.Version, statement pg_parse.Statement) []pg_contract.Error {
	var errors []pg_contract.Error

	for _, feature := range statement.Features {
		if version != pg_contract.VersionUnknown && version < feature.Since {
			errors = append(errors, pg_contract.Error{
				Code:    gateCode,
				Message: fmt.Sprintf("%s requires PostgreSQL %s, production runs %s", feature.Name, feature.Since, version),
			})
			continue
		}
	}

	return errors
}

// migrationVersions narrows a versionRange from the statements, used only when the production version isn't available.
func migrationVersions(versions pg_contract.VersionRange, statement pg_parse.Statement, errors []pg_contract.Error) pg_contract.VersionRange {
	versions = versions.Intersect(pg_contract.AtLeast(statement.MinVersion()))
	for _, err := range errors {
		versions = versions.Intersect(err.Versions)
	}
	return versions
}
