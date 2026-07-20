package database

import (
	"apercu-cli/config"
	"apercu-cli/helper/warning"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPreviewDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	prodConn, handler, err := GetPreviewDatabaseHandler(config.Database{Source: nil}, store)
	assert.NoError(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, prodConn)
	assert.Empty(t, store.GetWarningsRaw())
}

func TestGetPreviewDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	prodConn, handler, err := GetPreviewDatabaseHandler(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		PreviewBranch: "preview-1",
	}, store)
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, prodConn)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetPreviewDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	prodConn, handler, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
		PreviewBranch: "preview-1",
	}, store)
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Nil(t, prodConn)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetPruningDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	handler, err := GetPruningDatabaseHandler(config.Database{Source: nil}, store)
	assert.NoError(t, err)
	assert.Nil(t, handler)
	assert.Empty(t, store.GetWarningsRaw())
}

func TestGetPruningDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	handler, err := GetPruningDatabaseHandler(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		PreviewBranch: "preview-1",
	}, store)
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetPruningDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	handler, err := GetPruningDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
		PreviewBranch: "preview-1",
	}, store)
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetPruningDatabaseHandler_UsesSourceValuesWithoutAnonymization(t *testing.T) {
	store := warning.NewWarningStore()
	handler, err := GetPruningDatabaseHandler(config.Database{
		PreviewBranch: "preview-${{ PR_NUMBER }}",
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:    "src-project",
				ApiKey:       "src-key",
				ParentBranch: "main",
			},
		},
	}, store)
	require.NoError(t, err)
	require.NotNil(t, handler)
	assert.Empty(t, store.GetWarningsRaw())
	pruneHandler, ok := handler.(*NeonPruneHandler)
	require.True(t, ok)
	assert.Equal(t, "src-project", pruneHandler.projectId)
	assert.Equal(t, "src-key", pruneHandler.apiKey)
	assert.Equal(t, "main", pruneHandler.parentBranch)
	assert.Equal(t, "preview-${{ PR_NUMBER }}", pruneHandler.branchPattern)
}

func TestGetAnonymizationDatabaseHandlers_NilSourceOrAnonymization(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()

	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{Source: nil}, store)
	assert.NoError(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Empty(t, store.GetWarningsRaw())

	source, storage, err = GetAnonymizationDatabaseHandlers(config.Database{
		Source:        &config.DatabaseSource{Provider: config.DatabaseProviderNeon},
		Anonymization: nil,
	}, store)
	assert.NoError(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Empty(t, store.GetWarningsRaw())
}

func TestGetAnonymizationDatabaseHandlers_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		Anonymization: &config.DatabaseAnonymization{},
	}, store)
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetAnonymizationDatabaseHandlers_MissingNeonStorage(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     &config.DatabaseNeonSource{},
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{Neon: nil},
		},
	}, store)
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "missing storage database configuration")
}

func TestGetAnonymizationDatabaseHandlers_MissingNeonSource(t *testing.T) {
	t.Parallel()
	store := warning.NewWarningStore()
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{
				Neon: &config.DatabaseAnonymizationStorageNeon{BranchName: "anon"},
			},
		},
	}, store)
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Empty(t, store.GetWarningsRaw())
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}
