package database

import (
	"regexp"
	"testing"

	neon "github.com/kislerdm/neon-sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreviewPatternToRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pattern     string
		shouldMatch []string
		shouldNot   []string
	}{
		{
			name:        "single variable",
			pattern:     "preview-${{ PR_NUMBER }}",
			shouldMatch: []string{"preview-42", "preview-123", "preview-abc", "preview-"},
			shouldNot:   []string{"other-42", "xpreview-42"},
		},
		{
			name:        "variable with prefix and suffix",
			pattern:     "db-${{ PR_NUMBER }}-preview",
			shouldMatch: []string{"db-42-preview", "db-abc-preview", "db--preview"},
			shouldNot:   []string{"db-42", "42-preview"},
		},
		{
			name:        "multiple variables",
			pattern:     "${{ ENV }}-${{ PR_NUMBER }}",
			shouldMatch: []string{"staging-42", "prod-abc", "-42", "staging-"},
			shouldNot:   []string{"no-dash-match\nnewline"},
		},
		{
			name:        "no variables",
			pattern:     "static-branch",
			shouldMatch: []string{"static-branch"},
			shouldNot:   []string{"other-branch", "static-branch-extra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re, err := previewPatternToRegex(tt.pattern)
			require.NoError(t, err)

			for _, s := range tt.shouldMatch {
				assert.True(t, re.MatchString(s), "expected %q to match pattern %q", s, tt.pattern)
			}
			for _, s := range tt.shouldNot {
				assert.False(t, re.MatchString(s), "expected %q to NOT match pattern %q", s, tt.pattern)
			}
		})
	}
}

func TestFilterBranchesByParentAndPattern(t *testing.T) {
	t.Parallel()

	parentID := "parent-123"
	otherParent := "parent-456"
	pattern := regexp.MustCompile(`^preview-.*$`)

	branches := []neon.Branch{
		{Name: "preview-42", ID: "b1", ParentID: new(parentID)},
		{Name: "preview-99", ID: "b2", ParentID: new(parentID)},
		{Name: "preview-10", ID: "b3", ParentID: new(otherParent)}, // wrong parent
		{Name: "other-branch", ID: "b4", ParentID: new(parentID)},  // wrong pattern
		{Name: "preview-55", ID: "b5", ParentID: nil},              // nil parent
	}

	result := filterBranchesByParentAndPattern(branches, parentID, pattern)

	assert.Len(t, result, 2)
	assert.Equal(t, "preview-42", result[0].Name)
	assert.Equal(t, "preview-99", result[1].Name)
}

func TestFilterBranchesByParentAndPattern_Empty(t *testing.T) {
	t.Parallel()
	pattern := regexp.MustCompile(`^preview-.*$`)
	result := filterBranchesByParentAndPattern(nil, "parent", pattern)
	assert.Empty(t, result)
}

func TestSelectBranchesForPruning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		branches       []neon.Branch
		previewPattern string
		openPRs        []string
		expectedNames  []string
	}{
		{
			name: "keeps branches matching open PRs",
			branches: []neon.Branch{
				{Name: "preview-42", ID: "b1"},
				{Name: "preview-99", ID: "b2"},
				{Name: "preview-10", ID: "b3"},
			},
			previewPattern: "preview-${{ PR_NUMBER }}",
			openPRs:        []string{"42", "99"},
			expectedNames:  []string{"preview-10"},
		},
		{
			name: "prunes all when no open PRs",
			branches: []neon.Branch{
				{Name: "preview-42", ID: "b1"},
				{Name: "preview-99", ID: "b2"},
			},
			previewPattern: "preview-${{ PR_NUMBER }}",
			openPRs:        []string{},
			expectedNames:  []string{"preview-42", "preview-99"},
		},
		{
			name:           "empty branches returns empty",
			branches:       []neon.Branch{},
			previewPattern: "preview-${{ PR_NUMBER }}",
			openPRs:        []string{"42"},
			expectedNames:  []string{},
		},
		{
			name: "all branches match open PRs",
			branches: []neon.Branch{
				{Name: "preview-42", ID: "b1"},
			},
			previewPattern: "preview-${{ PR_NUMBER }}",
			openPRs:        []string{"42"},
			expectedNames:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := selectBranchesForPruning(tt.branches, tt.previewPattern, tt.openPRs)
			names := make([]string, len(result))
			for i, b := range result {
				names[i] = b.Name
			}
			assert.Equal(t, tt.expectedNames, names)
		})
	}
}
