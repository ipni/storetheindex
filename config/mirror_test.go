package config_test

import (
	"encoding/json"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"
)

func TestMirrorUnmarshalLegacyIdenticalToMain(t *testing.T) {
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
	require.Equal(t, config.MainModeReadWrite, m.MainMode)
	require.Equal(t, backend, m.Main)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyDistinctToMainAndExternal(t *testing.T) {
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
	// Distinct Retrieval covers reads via External, so MainMode is write-only.
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, storage, m.Main)
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
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, storage, m.Main)
	// Retrieval is ignored when Read is false, so External is not enabled.
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalLegacyDistinctReadOnlyUsesRetrievalAsMain(t *testing.T) {
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
	require.Equal(t, config.MainModeRead, m.MainMode)
	// Old read path used Retrieval; Storage was the unused write target.
	require.Equal(t, retrieval, m.Main)
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
	require.Equal(t, config.MainModeUnspecified, m.MainMode)
	require.Empty(t, m.Main.Type)
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
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, storage, m.Main)
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
	require.Equal(t, config.MainModeWrite, m.MainMode)
	require.Equal(t, storage, m.Main)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalMixedBackendStylesError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"MainMode": "write",
		"Main":     filestore.Config{Type: "s3"},
		"Storage":  filestore.Config{Type: "local"},
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedModeStylesError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"MainMode": "write",
		"Write":    true,
		"Main":     filestore.Config{Type: "local"},
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mixes legacy")
}

func TestMirrorUnmarshalMixedLegacyBackendWithNewModeError(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"MainMode": "write",
		"Storage":  filestore.Config{Type: "local"},
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
		"Main":  filestore.Config{Type: "local"},
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
	require.Equal(t, config.MainModeUnspecified, m.MainMode)
	require.Empty(t, m.Main.Type)
	require.Empty(t, m.External.Type)
}

func TestMirrorUnmarshalNewStyle(t *testing.T) {
	main := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/main",
		},
	}
	external := filestore.Config{
		Type: "http",
		HTTP: filestore.HTTPConfig{
			BaseURL: "http://example/carmirror/",
		},
	}
	data, err := json.Marshal(map[string]any{
		"MainMode": "readwrite",
		"Main":     main,
		"External": external,
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, config.MainModeReadWrite, m.MainMode)
	require.Equal(t, main, m.Main)
	require.Equal(t, external, m.External)
}

func TestMirrorUnmarshalInvalidMainMode(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"MainMode": "nope",
	})
	require.NoError(t, err)

	var m config.Mirror
	err = json.Unmarshal(data, &m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid AdvertisementMirror.MainMode")
}

func TestMirrorUnmarshalIgnoresFallbackRetrieval(t *testing.T) {
	main := filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: "/main",
		},
	}
	data, err := json.Marshal(map[string]any{
		"MainMode": "readwrite",
		"Main":     main,
		"FallbackRetrieval": filestore.Config{
			Type: "http",
			HTTP: filestore.HTTPConfig{BaseURL: "http://ignored/"},
		},
	})
	require.NoError(t, err)

	var m config.Mirror
	require.NoError(t, json.Unmarshal(data, &m))
	require.Equal(t, main, m.Main)
	require.Empty(t, m.External.Type)
}
