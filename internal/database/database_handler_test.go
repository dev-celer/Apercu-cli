package database

import (
	"apercu-cli/config"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPreviewDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	prodConn, handler, w, err := GetPreviewDatabaseHandler(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, prodConn)
	assert.Nil(t, w)
}

func TestGetPreviewDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	prodConn, handler, w, err := GetPreviewDatabaseHandler(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		PreviewBranch: "preview-1",
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, prodConn)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetPreviewDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	prodConn, handler, w, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
		PreviewBranch: "preview-1",
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, prodConn)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetPruningDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	handler, w, err := GetPruningDatabaseHandler(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, w)
}

func TestGetPruningDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	handler, w, err := GetPruningDatabaseHandler(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		PreviewBranch: "preview-1",
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetPruningDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	handler, w, err := GetPruningDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
		PreviewBranch: "preview-1",
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetPruningDatabaseHandler_UsesSourceValuesWithoutAnonymization(t *testing.T) {
	handler, w, err := GetPruningDatabaseHandler(config.Database{
		PreviewBranch: "preview-${{ PR_NUMBER }}",
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:    "src-project",
				ApiKey:       "src-key",
				ParentBranch: "main",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, handler)
	assert.Nil(t, w)
	pruneHandler, ok := handler.(*NeonPruneHandler)
	require.True(t, ok)
	assert.Equal(t, "src-project", pruneHandler.projectId)
	assert.Equal(t, "src-key", pruneHandler.apiKey)
	assert.Equal(t, "main", pruneHandler.parentBranch)
	assert.Equal(t, "preview-${{ PR_NUMBER }}", pruneHandler.branchPattern)
}

func TestGetAnonymizationDatabaseHandlers_NilSourceOrAnonymization(t *testing.T) {
	t.Parallel()

	source, storage, w, err := GetAnonymizationDatabaseHandlers(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Nil(t, w)

	source, storage, w, err = GetAnonymizationDatabaseHandlers(config.Database{
		Source:        &config.DatabaseSource{Provider: config.DatabaseProviderNeon},
		Anonymization: nil,
	})
	assert.NoError(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Nil(t, w)
}

func TestGetAnonymizationDatabaseHandlers_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	source, storage, w, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		Anonymization: &config.DatabaseAnonymization{},
	})
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetAnonymizationDatabaseHandlers_MissingNeonStorage(t *testing.T) {
	t.Parallel()
	source, storage, w, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     &config.DatabaseNeonSource{},
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{Neon: nil},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "missing storage database configuration")
}

func TestGetAnonymizationDatabaseHandlers_MissingNeonSource(t *testing.T) {
	t.Parallel()
	source, storage, w, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{
				Neon: &config.DatabaseAnonymizationStorageNeon{BranchName: "anon"},
			},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Nil(t, w)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}
