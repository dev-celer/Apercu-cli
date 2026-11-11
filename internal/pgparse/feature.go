package pgparse

import (
	"apercu-cli/internal/pgparse/ir"
)

// addFeature records a construct once. A statement that writes NOT ENFORCED on two constraints
// still bounds the version range once.
func addFeature(list []ir.Feature, name ir.FeatureName) []ir.Feature {
	for _, existing := range list {
		if existing.Name == name {
			return list
		}
	}
	return append(list, ir.NewFeature(name, ""))
}
