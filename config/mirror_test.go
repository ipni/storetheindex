package config_test

import (
	"encoding/json"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"
)

func TestMirrorUnmarshalLegacyIdenticalToLocal(t *testing.T) {
	backend := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/mirror",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Read":      true,
		"Write":     true,
		"Storage":   backend,
		"Retrieval": backend,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeReadWrite, m.LocalMode)
	require.Equal(t, backend, m.Local)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyDistinctToLocalAndExternal(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/write",
		},
	}
	retrieval := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/read",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Read":      true,
		"Write":     true,
		"Storage":   storage,
		"Retrieval": retrieval,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	// Distinct Retrieval covers reads via External, so LocalMode is write-only.
	require.Equal(t, config.LocalModeWrite, m.LocalMode)
	require.Equal(t, storage, m.Local)
	require.Equal(t, retrieval, m.External)
}

func TestMirrorUnmarshalLegacyDistinctWriteOnly(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/write",
		},
	}
	retrieval := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/read",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Write":     true,
		"Storage":   storage,
		"Retrieval": retrieval,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeWrite, m.LocalMode)
	require.Equal(t, storage, m.Local)
	// Retrieval is ignored when Read is false, so External is not enabled.
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyDistinctReadOnlyUsesRetrievalAsLocal(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/write",
		},
	}
	retrieval := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/read",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Read":      true,
		"Storage":   storage,
		"Retrieval": retrieval,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeRead, m.LocalMode)
	// Old read path used Retrieval; Storage was the unused write target.
	require.Equal(t, retrieval, m.Local)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyDistinctUnusedSkipsConversion(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/write",
		},
	}
	retrieval := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/read",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Storage":   storage,
		"Retrieval": retrieval,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeUnspecified, m.LocalMode)
	require.Empty(t, m.Local.Type)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyStorageOnly(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/write",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Write":   true,
		"Storage": storage,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeWrite, m.LocalMode)
	require.Equal(t, storage, m.Local)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyStorageOnlyMasksRead(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/write",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Read":    true,
		"Write":   true,
		"Storage": storage,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeWrite, m.LocalMode)
	require.Equal(t, storage, m.Local)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalMixedBackendStylesError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"LocalMode": "write",
		"Local":     filestore.Config{Type: "s3"},
		"Storage":   filestore.Config{Type: "local"},
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedModeStylesError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"LocalMode": "write",
		"Write":     true,
		"Local":     filestore.Config{Type: "local"},
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedLegacyBackendWithNewModeError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"LocalMode": "write",
		"Storage":   filestore.Config{Type: "local"},
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedLegacyModeWithNewBackendError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"Write": true,
		"Local": filestore.Config{Type: "local"},
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalLegacySkippedWhenUnused(t *testing.T) {
	storage := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/mirror",
		},
	}
	data, err := json.Marshal(map[string]any{
		"Storage":   storage,
		"Retrieval": storage,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeUnspecified, m.LocalMode)
	require.Empty(t, m.Local.Type)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalNewStyle(t *testing.T) {
	local := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/local",
		},
	}
	external := filestore.Config{
		Type: "http",
		HTTP: filestore.HTTPConfig{
			BaseURL: "http://example/carmirror/",
		},
	}
	data, err := json.Marshal(map[string]any{
		"LocalMode": "readwrite",
		"Local":     local,
		"External":  external,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.LocalModeReadWrite, m.LocalMode)
	require.Equal(t, local, m.Local)
	require.Equal(t, external, m.External)
}

func TestMirrorUnmarshalInvalidLocalMode(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"LocalMode": "nope",
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid AdvertisementMirror.LocalMode")
}

func TestMirrorUnmarshalIgnoresFallbackRetrieval(t *testing.T) {
	local := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/local",
		},
	}
	data, err := json.Marshal(map[string]any{
		"LocalMode": "readwrite",
		"Local":     local,
		"FallbackRetrieval": filestore.Config{
			Type: "http",
			HTTP: filestore.HTTPConfig{BaseURL: "http://ignored/"},
		},
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, local, m.Local)
	require.Empty(t, m.External.Type)
}
