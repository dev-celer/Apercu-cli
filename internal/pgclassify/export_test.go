package pgclassify

import "apercu-cli/internal/pgclassify/shadow"

// Checks is the constraint shadow the classifier has built so far, for the tests that read it back.
func (c *Classifier) Checks() *shadow.Checks { return c.checks }

var LevelGrading = levelGrading
