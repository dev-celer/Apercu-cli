package database

import (
	"apercu-cli/config"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPreviewDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	handler, err := GetPreviewDatabaseHandler(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, handler)
}

func TestGetPreviewDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	handler, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{Provider: "unknown"},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetPreviewDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	handler, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetPreviewDatabaseHandler_UsesSourceValuesWithoutAnonymization(t *testing.T) {
	t.Parallel()
	handler, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:     "src-project",
				ApiKey:        "src-key",
				ParentBranch:  "main",
				PreviewBranch: "preview-1",
				BranchingType: config.DatabaseNeonBranchingTypeSchemaOnly,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, handler)
	neonHandler, ok := handler.(*NeonHandler)
	require.True(t, ok)
	assert.Equal(t, "src-project", neonHandler.projectId)
	assert.Equal(t, "src-key", neonHandler.apiKey)
	assert.Equal(t, "preview-1", neonHandler.branch)
	require.NotNil(t, neonHandler.parentBranch)
	assert.Equal(t, "main", *neonHandler.parentBranch)
	require.NotNil(t, neonHandler.branchingType)
	assert.Equal(t, config.DatabaseNeonBranchingTypeSchemaOnly, *neonHandler.branchingType)
}

func TestGetPreviewDatabaseHandler_DefaultsInvalidBranchingType(t *testing.T) {
	t.Parallel()
	handler, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:     "p",
				ApiKey:        "k",
				ParentBranch:  "main",
				PreviewBranch: "preview",
				BranchingType: "bogus",
			},
		},
	})
	require.NoError(t, err)
	neonHandler := handler.(*NeonHandler)
	require.NotNil(t, neonHandler.branchingType)
	assert.Equal(t, config.DatabaseNeonBranchingTypeParentData, *neonHandler.branchingType)
}

func TestGetPreviewDatabaseHandler_OverridesWithAnonymizationStorage(t *testing.T) {
	t.Parallel()
	storageProject := "storage-project"
	storageApiKey := "storage-key"
	handler, err := GetPreviewDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:     "src-project",
				ApiKey:        "src-key",
				ParentBranch:  "main",
				PreviewBranch: "preview",
				BranchingType: config.DatabaseNeonBranchingTypeParentData,
			},
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{
				Neon: &config.DatabaseAnonymizationStorageNeon{
					ProjectId:  &storageProject,
					ApiKey:     &storageApiKey,
					BranchName: "main-anonymized",
				},
			},
		},
	})
	require.NoError(t, err)
	neonHandler := handler.(*NeonHandler)
	assert.Equal(t, storageProject, neonHandler.projectId)
	assert.Equal(t, storageApiKey, neonHandler.apiKey)
	require.NotNil(t, neonHandler.parentBranch)
	assert.Equal(t, "main-anonymized", *neonHandler.parentBranch)
}

func TestGetPruningDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	handler, err := GetPruningDatabaseHandler(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, handler)
}

func TestGetPruningDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	handler, err := GetPruningDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{Provider: "unknown"},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "unsupported database provider")
}

func TestGetPruningDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	handler, err := GetPruningDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetPruningDatabaseHandler_UsesSourceValuesWithoutAnonymization(t *testing.T) {
	handler, err := GetPruningDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:     "src-project",
				ApiKey:        "src-key",
				ParentBranch:  "main",
				PreviewBranch: "preview-${{ PR_NUMBER }}",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, handler)
	pruneHandler, ok := handler.(*NeonPruneHandler)
	require.True(t, ok)
	assert.Equal(t, "src-project", pruneHandler.projectId)
	assert.Equal(t, "src-key", pruneHandler.apiKey)
	assert.Equal(t, "main", pruneHandler.parentBranch)
	assert.Equal(t, "preview-${{ PR_NUMBER }}", pruneHandler.branchPattern)
}

func TestGetAnonymizationDatabaseHandlers_NilSourceOrAnonymization(t *testing.T) {
	t.Parallel()

	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)

	source, storage, err = GetAnonymizationDatabaseHandlers(config.Database{
		Source:        &config.DatabaseSource{Provider: config.DatabaseProviderNeon},
		Anonymization: nil,
	})
	assert.NoError(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
}

func TestGetAnonymizationDatabaseHandlers_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source:        &config.DatabaseSource{Provider: "unknown"},
		Anonymization: &config.DatabaseAnonymization{},
	})
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Contains(t, err.Error(), "unsupported database provider")
}

func TestGetAnonymizationDatabaseHandlers_MissingNeonStorage(t *testing.T) {
	t.Parallel()
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
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
	assert.Contains(t, err.Error(), "missing neon anonymization storage configuration")
}

func TestGetAnonymizationDatabaseHandlers_MissingNeonSource(t *testing.T) {
	t.Parallel()
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
	})
	assert.Error(t, err)
	assert.Nil(t, source)
	assert.Nil(t, storage)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetAnonymizationDatabaseHandlers_DefaultsStorageToSource(t *testing.T) {
	t.Parallel()
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:    "src-project",
				ApiKey:       "src-key",
				ParentBranch: "main",
			},
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{
				Neon: &config.DatabaseAnonymizationStorageNeon{
					BranchName: "main-anonymized",
				},
			},
		},
	})
	require.NoError(t, err)

	sourceHandler := source.(*NeonHandler)
	storageHandler := storage.(*NeonHandler)

	assert.Equal(t, "src-project", sourceHandler.projectId)
	assert.Equal(t, "src-key", sourceHandler.apiKey)
	assert.Equal(t, "main", sourceHandler.branch)
	assert.Nil(t, sourceHandler.parentBranch)
	assert.Nil(t, sourceHandler.branchingType)

	assert.Equal(t, "src-project", storageHandler.projectId)
	assert.Equal(t, "src-key", storageHandler.apiKey)
	assert.Equal(t, "main-anonymized", storageHandler.branch)
	require.NotNil(t, storageHandler.parentBranch)
	assert.Equal(t, "main", *storageHandler.parentBranch)
}

func TestGetAnonymizationDatabaseHandlers_OverridesStorageCredentials(t *testing.T) {
	t.Parallel()
	overrideProject := "storage-project"
	overrideApiKey := "storage-key"
	source, storage, err := GetAnonymizationDatabaseHandlers(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon: &config.DatabaseNeonSource{
				ProjectId:    "src-project",
				ApiKey:       "src-key",
				ParentBranch: "main",
			},
		},
		Anonymization: &config.DatabaseAnonymization{
			Storage: config.DatabaseAnonymizationStorage{
				Neon: &config.DatabaseAnonymizationStorageNeon{
					ProjectId:  &overrideProject,
					ApiKey:     &overrideApiKey,
					BranchName: "main-anonymized",
				},
			},
		},
	})
	require.NoError(t, err)

	sourceHandler := source.(*NeonHandler)
	storageHandler := storage.(*NeonHandler)

	assert.Equal(t, "src-project", sourceHandler.projectId)
	assert.Equal(t, "src-key", sourceHandler.apiKey)

	assert.Equal(t, overrideProject, storageHandler.projectId)
	assert.Equal(t, overrideApiKey, storageHandler.apiKey)
}
