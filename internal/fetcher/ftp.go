package fetcher

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/rotisserie/eris"
	"go.uber.org/zap"
)

// FTPOptions configures the FTP fetcher.
type FTPOptions struct {
	Timeout time.Duration
}

// FTPFetcher downloads files over FTP.
type FTPFetcher struct {
	opts FTPOptions
}

// NewFTPFetcher creates a new FTPFetcher with the given options.
func NewFTPFetcher(opts FTPOptions) *FTPFetcher {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	return &FTPFetcher{opts: opts}
}

// parseFTPURL extracts host (with port) and path from an FTP URL.
func parseFTPURL(rawURL string) (host string, path string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", eris.Wrap(err, "parse ftp url")
	}
	if u.Scheme != "ftp" {
		return "", "", eris.Errorf("expected ftp scheme, got %q", u.Scheme)
	}

	host = u.Host
	if _, _, splitErr := net.SplitHostPort(host); splitErr != nil {
		host = net.JoinHostPort(host, "21")
	}

	path = u.Path
	if path == "" {
		return "", "", eris.New("empty path in ftp url")
	}

	return host, path, nil
}

// ftpConnReader wraps an FTP response and connection so that closing the reader
// also closes the FTP response and disconnects from the server.
type ftpConnReader struct {
	resp *ftp.Response
	conn *ftp.ServerConn
}

func (r *ftpConnReader) Read(p []byte) (int, error) {
	return r.resp.Read(p)
}

func (r *ftpConnReader) Close() error {
	respErr := r.resp.Close()
	quitErr := r.conn.Quit()
	if respErr != nil {
		return eris.Wrap(respErr, "close ftp response")
	}
	if quitErr != nil {
		return eris.Wrap(quitErr, "quit ftp connection")
	}
	return nil
}

// Download connects to the FTP server, retrieves the file, and returns a reader.
// The caller must close the returned ReadCloser to release the FTP connection.
func (f *FTPFetcher) Download(ctx context.Context, ftpURL string) (io.ReadCloser, error) {
	host, path, err := parseFTPURL(ftpURL)
	if err != nil {
		return nil, err
	}

	zap.L().Debug("ftp: connecting", zap.String("host", host), zap.String("path", path))

	conn, err := ftp.Dial(host, ftp.DialWithTimeout(f.opts.Timeout), ftp.DialWithContext(ctx))
	if err != nil {
		return nil, eris.Wrap(err, "ftp dial")
	}

	if err := conn.Login("anonymous", "anonymous@"); err != nil {
		conn.Quit()
		return nil, eris.Wrap(err, "ftp login")
	}

	resp, err := conn.Retr(path)
	if err != nil {
		conn.Quit()
		return nil, eris.Wrap(err, "ftp retrieve")
	}

	return &ftpConnReader{resp: resp, conn: conn}, nil
}

// DownloadToFile downloads the FTP URL to a local file. Returns bytes written.
func (f *FTPFetcher) DownloadToFile(ctx context.Context, ftpURL string, path string) (int64, error) {
	rc, err := f.Download(ctx, ftpURL)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	file, err := os.Create(path)
	if err != nil {
		return 0, eris.Wrap(err, "create file")
	}
	defer file.Close()

	n, err := io.Copy(file, rc)
	if err != nil {
		return n, eris.Wrap(err, "write file")
	}

	return n, nil
}
