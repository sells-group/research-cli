package fetcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFTPURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantHost string
		wantPath string
		wantErr  bool
	}{
		{
			name:     "standard ftp url",
			url:      "ftp://ftp.example.com/pub/data/file.csv",
			wantHost: "ftp.example.com:21",
			wantPath: "/pub/data/file.csv",
		},
		{
			name:     "ftp url with port",
			url:      "ftp://ftp.example.com:2121/data/file.txt",
			wantHost: "ftp.example.com:2121",
			wantPath: "/data/file.txt",
		},
		{
			name:     "ftp url with nested path",
			url:      "ftp://ftp.sec.gov/edgar/full-index/2024/QTR1/company.idx",
			wantHost: "ftp.sec.gov:21",
			wantPath: "/edgar/full-index/2024/QTR1/company.idx",
		},
		{
			name:    "http scheme rejected",
			url:     "http://example.com/file.csv",
			wantErr: true,
		},
		{
			name:    "empty path",
			url:     "ftp://ftp.example.com",
			wantErr: true,
		},
		{
			name:    "invalid url",
			url:     "://bad",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, path, err := parseFTPURL(tt.url)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantPath, path)
		})
	}
}

func TestNewFTPFetcher_DefaultTimeout(t *testing.T) {
	f := NewFTPFetcher(FTPOptions{})
	assert.Equal(t, 30_000_000_000, int(f.opts.Timeout)) // 30s in nanoseconds
}
