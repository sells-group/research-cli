package fetcher

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// miniFTPServer is a minimal FTP server for testing.
// It supports just enough of the FTP protocol to test Download and DownloadToFile.
type miniFTPServer struct {
	listener net.Listener
	fileData map[string]string // path -> content
	wg       sync.WaitGroup
	mu       sync.Mutex
	closed   bool
}

func newMiniFTPServer(t *testing.T, files map[string]string) *miniFTPServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &miniFTPServer{
		listener: ln,
		fileData: files,
	}

	s.wg.Add(1)
	go s.serve(t)

	return s
}

func (s *miniFTPServer) addr() string {
	return s.listener.Addr().String()
}

func (s *miniFTPServer) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.listener.Close() //nolint:errcheck
	s.wg.Wait()
}

func (s *miniFTPServer) serve(t *testing.T) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.handleConn(t, conn)
	}
}

func (s *miniFTPServer) handleConn(_ *testing.T, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close() //nolint:errcheck

	conn.SetDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	// Send greeting
	fmt.Fprintf(writer, "220 Mini FTP Server ready\r\n") //nolint:errcheck
	writer.Flush()                                       //nolint:errcheck

	var dataListener net.Listener

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToUpper(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}

		switch cmd {
		case "USER":
			fmt.Fprintf(writer, "230 User logged in\r\n") //nolint:errcheck
			writer.Flush()                                //nolint:errcheck

		case "PASS":
			fmt.Fprintf(writer, "230 User logged in\r\n") //nolint:errcheck
			writer.Flush()                                //nolint:errcheck

		case "FEAT":
			fmt.Fprintf(writer, "211-Features:\r\n") //nolint:errcheck
			fmt.Fprintf(writer, " UTF8\r\n")         //nolint:errcheck
			fmt.Fprintf(writer, "211 End\r\n")       //nolint:errcheck
			writer.Flush()                           //nolint:errcheck

		case "TYPE":
			fmt.Fprintf(writer, "200 Type set to %s\r\n", arg) //nolint:errcheck
			writer.Flush()                                     //nolint:errcheck

		case "EPSV":
			// Open a data connection listener
			var err error
			dataListener, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				fmt.Fprintf(writer, "425 Can't open data connection\r\n") //nolint:errcheck
				writer.Flush()                                            //nolint:errcheck
				continue
			}
			port := dataListener.Addr().(*net.TCPAddr).Port
			fmt.Fprintf(writer, "229 Entering Extended Passive Mode (|||%d|)\r\n", port) //nolint:errcheck
			writer.Flush()                                                               //nolint:errcheck

		case "PASV":
			// Open a data connection listener
			var err error
			dataListener, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				fmt.Fprintf(writer, "425 Can't open data connection\r\n") //nolint:errcheck
				writer.Flush()                                            //nolint:errcheck
				continue
			}
			addr := dataListener.Addr().(*net.TCPAddr)
			p1 := addr.Port / 256
			p2 := addr.Port % 256
			fmt.Fprintf(writer, "227 Entering Passive Mode (127,0,0,1,%d,%d)\r\n", p1, p2) //nolint:errcheck
			writer.Flush()                                                                 //nolint:errcheck

		case "RETR":
			if dataListener == nil {
				fmt.Fprintf(writer, "425 Use PASV first\r\n") //nolint:errcheck
				writer.Flush()                                //nolint:errcheck
				continue
			}

			content, ok := s.fileData[arg]
			if !ok {
				fmt.Fprintf(writer, "550 File not found\r\n") //nolint:errcheck
				writer.Flush()                                //nolint:errcheck
				dataListener.Close()                          //nolint:errcheck
				dataListener = nil
				continue
			}

			fmt.Fprintf(writer, "150 Opening data connection\r\n") //nolint:errcheck
			writer.Flush()                                         //nolint:errcheck

			dataConn, err := dataListener.Accept()
			if err != nil {
				fmt.Fprintf(writer, "425 Can't open data connection\r\n") //nolint:errcheck
				writer.Flush()                                            //nolint:errcheck
				continue
			}

			io.WriteString(dataConn, content) //nolint:errcheck
			dataConn.Close()                  //nolint:errcheck
			dataListener.Close()              //nolint:errcheck
			dataListener = nil

			fmt.Fprintf(writer, "226 Transfer complete\r\n") //nolint:errcheck
			writer.Flush()                                   //nolint:errcheck

		case "QUIT":
			fmt.Fprintf(writer, "221 Goodbye\r\n") //nolint:errcheck
			writer.Flush()                         //nolint:errcheck
			return

		case "OPTS":
			fmt.Fprintf(writer, "200 OK\r\n") //nolint:errcheck
			writer.Flush()                    //nolint:errcheck

		default:
			fmt.Fprintf(writer, "502 Command not implemented\r\n") //nolint:errcheck
			writer.Flush()                                         //nolint:errcheck
		}
	}
}

func TestFTPFetcher_Download(t *testing.T) {
	srv := newMiniFTPServer(t, map[string]string{
		"/data/test.csv": "a,b,c\n1,2,3\n",
	})
	defer srv.close()

	f := NewFTPFetcher(FTPOptions{Timeout: 5 * time.Second})

	ftpURL := fmt.Sprintf("ftp://%s/data/test.csv", srv.addr())
	body, err := f.Download(context.Background(), ftpURL)
	require.NoError(t, err)
	defer body.Close() //nolint:errcheck

	data, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "a,b,c\n1,2,3\n", string(data))
}

func TestFTPFetcher_DownloadToFile(t *testing.T) {
	srv := newMiniFTPServer(t, map[string]string{
		"/data/file.txt": "hello ftp world",
	})
	defer srv.close()

	f := NewFTPFetcher(FTPOptions{Timeout: 5 * time.Second})

	dir := t.TempDir()
	destPath := filepath.Join(dir, "output.txt")

	ftpURL := fmt.Sprintf("ftp://%s/data/file.txt", srv.addr())
	n, err := f.DownloadToFile(context.Background(), ftpURL, destPath)
	require.NoError(t, err)
	assert.Equal(t, int64(15), n)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, "hello ftp world", string(data))
}

func TestFTPFetcher_Download_InvalidURL(t *testing.T) {
	f := NewFTPFetcher(FTPOptions{Timeout: 5 * time.Second})

	_, err := f.Download(context.Background(), "http://not-ftp/path")
	require.Error(t, err)
}

func TestFTPFetcher_Download_ConnectionRefused(t *testing.T) {
	f := NewFTPFetcher(FTPOptions{Timeout: 2 * time.Second})

	// Use a port that nothing is listening on
	_, err := f.Download(context.Background(), "ftp://127.0.0.1:19999/path/file.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ftp dial")
}

func TestFTPFetcher_Download_FileNotFound(t *testing.T) {
	srv := newMiniFTPServer(t, map[string]string{
		"/existing.txt": "data",
	})
	defer srv.close()

	f := NewFTPFetcher(FTPOptions{Timeout: 5 * time.Second})

	ftpURL := fmt.Sprintf("ftp://%s/nonexistent.txt", srv.addr())
	_, err := f.Download(context.Background(), ftpURL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ftp retrieve")
}

func TestFTPFetcher_DownloadToFile_CreateFileError(t *testing.T) {
	srv := newMiniFTPServer(t, map[string]string{
		"/data.txt": "content",
	})
	defer srv.close()

	f := NewFTPFetcher(FTPOptions{Timeout: 5 * time.Second})

	ftpURL := fmt.Sprintf("ftp://%s/data.txt", srv.addr())
	_, err := f.DownloadToFile(context.Background(), ftpURL, "/nonexistent/dir/file.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create file")
}

func TestFTPFetcher_DownloadToFile_DownloadError(t *testing.T) {
	f := NewFTPFetcher(FTPOptions{Timeout: 2 * time.Second})

	_, err := f.DownloadToFile(context.Background(), "ftp://127.0.0.1:19999/file.txt", "/tmp/out.txt")
	require.Error(t, err)
}

func TestFTPConnReader_ReadAndClose(t *testing.T) {
	srv := newMiniFTPServer(t, map[string]string{
		"/test.txt": "read close test",
	})
	defer srv.close()

	f := NewFTPFetcher(FTPOptions{Timeout: 5 * time.Second})

	ftpURL := fmt.Sprintf("ftp://%s/test.txt", srv.addr())
	rc, err := f.Download(context.Background(), ftpURL)
	require.NoError(t, err)

	// Read partial data
	buf := make([]byte, 4)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "read", string(buf))

	// Close the connection (tests ftpConnReader.Close)
	err = rc.Close()
	require.NoError(t, err)
}
