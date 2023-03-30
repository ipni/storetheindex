package httpfindserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_explicitlyAcceptsNDJson(t *testing.T) {
	tests := []struct {
		name    string
		given   string
		want    bool
		wantErr bool
	}{
		{
			name:    "browser",
			given:   "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
			wantErr: true,
		},
		{
			name:    "extra space",
			given:   "ext/html,application/xhtml+xml   ,   application/xml;q=0.9",
			wantErr: true,
		},
		{
			name:    "none",
			wantErr: true,
		},
		{
			name:    "invalid",
			given:   `;;;;`,
			wantErr: true,
		},
		{
			name:  "extra space ndjson",
			given: "ext/html,application/xhtml+xml   ,   application/x-ndjson;q=0.9",
			want:  true,
		},
		{
			name:    "json",
			given:   "application/json",
			wantErr: true,
		},
		{
			name:  "ndjson",
			given: "application/x-ndjson",
			want:  true,
		},
	}
	w := httptest.NewRecorder()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "fish.invalid", nil)
			require.NoError(t, err)
			if tt.given != "" {
				r.Header.Set("Accept", tt.given)
			}
			match, ok := acceptsAnyOf(w, r, true, mediaTypeNDJson)
			if ok == tt.wantErr {
				t.Errorf("getAccepts() hasError = %v, wantErr %v", !ok, tt.wantErr)
				return
			}
			got := match != ""
			require.Equal(t, got, tt.want, "getAccepts() got = %v, want %v", got, tt.want)
		})
	}
}

func Test_acceptsAnyOf(t *testing.T) {
	tests := []struct {
		name        string
		givenHeader string
		givenAnyOf  []string
		want        string
		wantErr     bool
	}{
		{
			name:        "empty match",
			givenHeader: "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
			wantErr:     true,
		},
		{
			name:        "no match",
			givenHeader: "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
			givenAnyOf:  []string{"application/json"},
			wantErr:     true,
		},
		{
			name:        "extra space with match",
			givenHeader: "ext/html,application/xhtml+xml   ,   application/xml;q=0.9",
			givenAnyOf:  []string{"application/json", "application/xhtml+xml"},
			want:        "application/xhtml+xml",
		},
		{
			name: "none",
		},
		{
			name:        "invalid",
			givenHeader: `;;;;`,
			wantErr:     true,
		},
		{
			name:        "extra space ndjson",
			givenHeader: "ext/html,application/xhtml+xml   ,   application/x-ndjson;q=0.9",
			givenAnyOf:  []string{"application/x-ndjson"},
			want:        "application/x-ndjson",
		},
		{
			name:        "json repeated",
			givenHeader: "application/x-ndjson,   application/x-ndjson;q=0.9, application/json",
			givenAnyOf:  []string{"application/json"},
			want:        "application/json",
		},
		{
			name:        "ndjson",
			givenHeader: "application/json,   application/x-ndjson;q=0.9",
			givenAnyOf:  []string{"application/x-ndjson", "application/json"},
			want:        "application/x-ndjson",
		},
	}
	w := httptest.NewRecorder()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "fish.invalid", nil)
			require.NoError(t, err)
			if tt.givenHeader != "" {
				r.Header.Set("Accept", tt.givenHeader)
			}
			firstMatch, ok := acceptsAnyOf(w, r, true, tt.givenAnyOf...)
			if ok == tt.wantErr {
				t.Errorf("getAccepts() hasError = %v, wantErr %v", !ok, tt.wantErr)
				return
			}
			require.Equal(t, firstMatch, tt.want, "getAccepts() firstMatch = %v, want %v", firstMatch, tt.want)
		})
	}
}
