package anonymization

import (
	"apercu-cli/config"
	"apercu-cli/helper"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDatabaseAnonymizer_NilSource(t *testing.T) {
	t.Parallel()
	handler := GetDatabaseAnonymizer(config.Database{Source: nil}, helper.ConnectionFields{}, helper.ConnectionFields{})
	assert.Nil(t, handler)
}

func TestGetDatabaseAnonymizer_NilAnonymization(t *testing.T) {
	t.Parallel()
	handler := GetDatabaseAnonymizer(config.Database{
		Source:        &config.DatabaseSource{Provider: config.DatabaseProviderNeon},
		Anonymization: nil,
	}, helper.ConnectionFields{}, helper.ConnectionFields{})
	assert.Nil(t, handler)
}

func TestGetDatabaseAnonymizer_BuildsGreenmaskHandler(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://resolved-url")

	sourceConn := helper.ConnectionFields{Host: "src", Port: 5432, User: "u", Password: "p", Database: "d"}
	storageConn := helper.ConnectionFields{Host: "dst", Port: 5432, User: "u", Password: "p", Database: "d"}

	handler := GetDatabaseAnonymizer(config.Database{
		Source: &config.DatabaseSource{Provider: config.DatabaseProviderNeon},
		Anonymization: &config.DatabaseAnonymization{
			GreenmaskConfig: "./greenmask.yaml",
			Env: map[string]string{
				"DATABASE_URL": "${{ DATABASE_URL }}",
				"STATIC":       "static-value",
			},
		},
	}, sourceConn, storageConn)

	require.NotNil(t, handler)
	greenmask, ok := handler.(*GreenmaskHandler)
	require.True(t, ok)
	assert.Equal(t, sourceConn, greenmask.sourceConnection)
	assert.Equal(t, storageConn, greenmask.storageConnection)
	assert.Equal(t, "./greenmask.yaml", greenmask.configPath)
	assert.Equal(t, "postgres://resolved-url", greenmask.env["DATABASE_URL"])
	assert.Equal(t, "static-value", greenmask.env["STATIC"])
}
