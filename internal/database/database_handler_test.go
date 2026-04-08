package database

import (
	"apercu-cli/config"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSourceDatabaseHandler_NilSource(t *testing.T) {
	t.Parallel()
	handler, err := GetSourceDatabaseHandler(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, handler)
}

func TestGetSourceDatabaseHandler_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	handler, err := GetSourceDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{Provider: "unknown"},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "unsupported source database provider")
}

func TestGetSourceDatabaseHandler_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	handler, err := GetSourceDatabaseHandler(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}

func TestGetDatabaseHandlerForPruning_NilSource(t *testing.T) {
	t.Parallel()
	handler, err := GetDatabaseHandlerForPruning(config.Database{Source: nil})
	assert.NoError(t, err)
	assert.Nil(t, handler)
}

func TestGetDatabaseHandlerForPruning_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	handler, err := GetDatabaseHandlerForPruning(config.Database{
		Source: &config.DatabaseSource{Provider: "unknown"},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "unsupported database provider")
}

func TestGetDatabaseHandlerForPruning_MissingNeonConfig(t *testing.T) {
	t.Parallel()
	handler, err := GetDatabaseHandlerForPruning(config.Database{
		Source: &config.DatabaseSource{
			Provider: config.DatabaseProviderNeon,
			Neon:     nil,
		},
	})
	assert.Error(t, err)
	assert.Nil(t, handler)
	assert.Contains(t, err.Error(), "missing neon source database configuration")
}
