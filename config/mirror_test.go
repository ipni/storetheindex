package config_test

import (
	"encoding/json"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"
)

func TestMirrorUnmarshalLegacyIdenticalToMain(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Read": true,
		"Write": true,
		"Storage": {"Type": "local", "Local": {"BasePath": "/mirror"}},
		"Retrieval": {"Type": "local", "Local": {"BasePath": "/mirror"}}
	}`), &m))
	require.Equal(t, config.MainModeReadWrite, m.MainMode)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/mirror"},
	}, m.Main.Config)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalLegacyDistinctToMainAndExternal(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Read": true,
		"Write": true,
		"Storage": {"Type": "local", "Local": {"BasePath": "/write"}},
		"Retrieval": {"Type": "local", "Local": {"BasePath": "/read"}}
	}`), &m))
	// Distinct Retrieval covers reads via External, so MainMode is write-only.
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/write"},
	}, m.Main.Config)
	require.Len(t, m.External, 1)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/read"},
	}, m.External[0].Config)
}

func TestMirrorUnmarshalLegacyDistinctWriteOnly(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Write": true,
		"Storage": {"Type": "local", "Local": {"BasePath": "/write"}},
		"Retrieval": {"Type": "local", "Local": {"BasePath": "/read"}}
	}`), &m))
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/write"},
	}, m.Main.Config)
	// Retrieval is ignored when Read is false, so External is not enabled.
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalLegacyDistinctReadOnlyUsesRetrievalAsMain(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Read": true,
		"Storage": {"Type": "local", "Local": {"BasePath": "/write"}},
		"Retrieval": {"Type": "local", "Local": {"BasePath": "/read"}}
	}`), &m))
	require.Equal(t, config.MainModeRead, m.MainMode)
	// Old read path used Retrieval; Storage was the unused write target.
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/read"},
	}, m.Main.Config)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalLegacyDistinctUnusedSkipsConversion(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Storage": {"Type": "local", "Local": {"BasePath": "/write"}},
		"Retrieval": {"Type": "local", "Local": {"BasePath": "/read"}}
	}`), &m))
	require.Equal(t, config.MainModeUnspecified, m.MainMode)
	require.Empty(t, m.Main.Type)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalLegacyStorageOnly(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Write": true,
		"Storage": {"Type": "local", "Local": {"BasePath": "/write"}}
	}`), &m))
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/write"},
	}, m.Main.Config)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalLegacyStorageOnlyMasksRead(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Read": true,
		"Write": true,
		"Storage": {"Type": "local", "Local": {"BasePath": "/write"}}
	}`), &m))
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/write"},
	}, m.Main.Config)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalMixedBackendStylesError(t *testing.T) {
	var m config.Mirror
	err := json.Unmarshal([]byte(`{
		"MainMode": "write",
		"Main": {"Type": "s3"},
		"Storage": {"Type": "local"}
	}`), &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedModeStylesError(t *testing.T) {
	var m config.Mirror
	err := json.Unmarshal([]byte(`{
		"MainMode": "write",
		"Write": true,
		"Main": {"Type": "local"}
	}`), &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedLegacyBackendWithNewModeError(t *testing.T) {
	var m config.Mirror
	err := json.Unmarshal([]byte(`{
		"MainMode": "write",
		"Storage": {"Type": "local"}
	}`), &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedLegacyModeWithNewBackendError(t *testing.T) {
	var m config.Mirror
	err := json.Unmarshal([]byte(`{
		"Write": true,
		"Main": {"Type": "local"}
	}`), &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalLegacySkippedWhenUnused(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Storage": {"Type": "local", "Local": {"BasePath": "/mirror"}},
		"Retrieval": {"Type": "local", "Local": {"BasePath": "/mirror"}}
	}`), &m))
	require.Equal(t, config.MainModeUnspecified, m.MainMode)
	require.Empty(t, m.Main.Type)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalNewStyleExternal(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"MainMode": "readwrite",
		"Main": {
			"Type": "local",
			"Local": {"BasePath": "/main"},
			"Compress": "gzip"
		},
		"External": [{
			"Type": "http",
			"HTTP": {"BaseURL": "http://example/carmirror/"},
			"Compress": "none"
		}]
	}`), &m))
	require.Equal(t, config.MainModeReadWrite, m.MainMode)
	require.Equal(t, config.StoreConfig{
		Config: filestore.Config{
			Type:  "local",
			Local: filestore.LocalConfig{BasePath: "/main"},
		},
		Compress: "gzip",
	}, m.Main)
	require.Len(t, m.External, 1)
	require.Equal(t, config.StoreConfig{
		Config: filestore.Config{
			Type: "http",
			HTTP: filestore.HTTPConfig{BaseURL: "http://example/carmirror/"},
		},
		Compress: "none",
	}, m.External[0])
}

func TestMirrorUnmarshalNewStyleArrayExternal(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"MainMode": "write",
		"Main": {"Type": "local", "Local": {"BasePath": "/main"}},
		"External": [
			{"Type": "http", "HTTP": {"BaseURL": "http://a/carmirror/"}, "Compress": "gzip"},
			{"Type": "http", "HTTP": {"BaseURL": "http://b/carmirror/"}, "Compress": "gzip"}
		]
	}`), &m))
	require.Len(t, m.External, 2)
	require.Equal(t, "http://a/carmirror/", m.External[0].HTTP.BaseURL)
	require.Equal(t, "http://b/carmirror/", m.External[1].HTTP.BaseURL)
}

func TestMirrorUnmarshalLegacyTopLevelCompress(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Compress": "gzip",
		"MainMode": "readwrite",
		"Main": {"Type": "local", "Local": {"BasePath": "/main"}},
		"External": [{"Type": "http", "HTTP": {"BaseURL": "http://example/carmirror/"}}]
	}`), &m))
	require.Equal(t, "gzip", m.Main.Compress)
	require.Len(t, m.External, 1)
	require.Equal(t, "gzip", m.External[0].Compress)
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/main"},
	}, m.Main.Config)
	require.Equal(t, filestore.Config{
		Type: "http",
		HTTP: filestore.HTTPConfig{BaseURL: "http://example/carmirror/"},
	}, m.External[0].Config)
}

func TestMirrorUnmarshalLegacyTopLevelCompressMainOnly(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"Compress": "none",
		"MainMode": "write",
		"Main": {"Type": "local", "Local": {"BasePath": "/main"}}
	}`), &m))
	require.Equal(t, "none", m.Main.Compress)
	require.Nil(t, m.External)
}

func TestMirrorUnmarshalMixedCompressStylesError(t *testing.T) {
	var m config.Mirror
	err := json.Unmarshal([]byte(`{
		"Compress": "gzip",
		"MainMode": "write",
		"Main": {"Type": "local", "Compress": "none"}
	}`), &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy top-level Compress")
}

func TestMirrorUnmarshalInvalidMainMode(t *testing.T) {
	var m config.Mirror
	err := json.Unmarshal([]byte(`{"MainMode": "nope"}`), &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid AdvertisementMirror.MainMode")
}

func TestMirrorUnmarshalIgnoresFallbackRetrieval(t *testing.T) {
	var m config.Mirror
	require.NoError(t, json.Unmarshal([]byte(`{
		"MainMode": "readwrite",
		"Main": {"Type": "local", "Local": {"BasePath": "/main"}},
		"FallbackRetrieval": {"Type": "http", "HTTP": {"BaseURL": "http://ignored/"}}
	}`), &m))
	require.Equal(t, filestore.Config{
		Type:  "local",
		Local: filestore.LocalConfig{BasePath: "/main"},
	}, m.Main.Config)
	require.Nil(t, m.External)
}
