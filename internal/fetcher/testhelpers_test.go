package fetcher

import "os"

// writeTestFile is a helper that writes data to a file path.
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
