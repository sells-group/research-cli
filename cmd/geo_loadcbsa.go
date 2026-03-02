package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/sells-group/research-cli/internal/geo"
)

var geoLoadCBSACmd = &cobra.Command{
	Use:   "load-cbsa",
	Short: "Load CBSA shapefiles into database",
	Long:  "Downloads Census Bureau CBSA shapefiles and loads into the public.cbsa_areas table.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		tempDir, _ := cmd.Flags().GetString("temp-dir")
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return eris.Wrapf(err, "geo: create temp dir %s", tempDir)
		}
		defer func() {
			_ = os.RemoveAll(tempDir)
		}()

		httpClient := &http.Client{Timeout: 10 * time.Minute}

		zap.L().Info("loading CBSA shapefiles", zap.String("temp_dir", tempDir))

		if err := geo.ImportCBSA(ctx, pool, httpClient, tempDir); err != nil {
			return eris.Wrap(err, "geo load-cbsa")
		}

		fmt.Println("CBSA shapefiles loaded successfully")
		return nil
	},
}

func init() {
	geoLoadCBSACmd.Flags().String("temp-dir", "/tmp/geo", "temporary directory for shapefile downloads")
	geoCmd.AddCommand(geoLoadCBSACmd)
}
