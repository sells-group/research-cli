package main

import (
	"os/signal"
	"syscall"

	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
)

var geoStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show geocoding statistics",
	Long:  "Display address geocoding and MSA association statistics.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		pool, err := fedsyncPool(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		// Total addresses.
		var totalAddresses int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM company_addresses`).Scan(&totalAddresses)
		if err != nil {
			return eris.Wrap(err, "geo status: count addresses")
		}

		// Geocoded addresses.
		var geocoded int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM company_addresses WHERE geocoded_at IS NOT NULL`).Scan(&geocoded)
		if err != nil {
			return eris.Wrap(err, "geo status: count geocoded")
		}

		// Ungeocoded addresses.
		ungeocoded := totalAddresses - geocoded

		// MSA association count.
		var msaAssociations int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM address_msa`).Scan(&msaAssociations)
		if err != nil {
			return eris.Wrap(err, "geo status: count MSA associations")
		}

		// Classification breakdown.
		type classCount struct {
			Classification string
			Count          int
		}
		rows, err := pool.Query(ctx, `
			SELECT classification, COUNT(*) AS cnt
			FROM address_msa
			GROUP BY classification
			ORDER BY cnt DESC`)
		if err != nil {
			return eris.Wrap(err, "geo status: classification breakdown")
		}
		defer rows.Close()

		var classifications []classCount
		for rows.Next() {
			var cc classCount
			if err := rows.Scan(&cc.Classification, &cc.Count); err != nil {
				return eris.Wrap(err, "geo status: scan classification")
			}
			classifications = append(classifications, cc)
		}
		if err := rows.Err(); err != nil {
			return eris.Wrap(err, "geo status: iterate classifications")
		}

		// Geocode source breakdown.
		type sourceCount struct {
			Source string
			Count  int
		}
		srcRows, err := pool.Query(ctx, `
			SELECT COALESCE(geocode_source, 'unknown') AS src, COUNT(*) AS cnt
			FROM company_addresses
			WHERE geocoded_at IS NOT NULL
			GROUP BY geocode_source
			ORDER BY cnt DESC`)
		if err != nil {
			return eris.Wrap(err, "geo status: source breakdown")
		}
		defer srcRows.Close()

		var sources []sourceCount
		for srcRows.Next() {
			var sc sourceCount
			if err := srcRows.Scan(&sc.Source, &sc.Count); err != nil {
				return eris.Wrap(err, "geo status: scan source")
			}
			sources = append(sources, sc)
		}
		if err := srcRows.Err(); err != nil {
			return eris.Wrap(err, "geo status: iterate sources")
		}

		// Print summary.
		printOutputln(cmd, "=== Geocoding Status ===")
		printOutputf(cmd, "Total addresses:    %d\n", totalAddresses)
		printOutputf(cmd, "Geocoded:           %d\n", geocoded)
		printOutputf(cmd, "Ungeocoded:         %d\n", ungeocoded)
		printOutputf(cmd, "MSA associations:   %d\n", msaAssociations)
		printOutputln(cmd)

		if len(sources) > 0 {
			printOutputln(cmd, "Geocode sources:")
			for _, s := range sources {
				printOutputf(cmd, "  %-15s %d\n", s.Source, s.Count)
			}
			printOutputln(cmd)
		}

		if len(classifications) > 0 {
			printOutputln(cmd, "Classification breakdown:")
			for _, c := range classifications {
				printOutputf(cmd, "  %-15s %d\n", c.Classification, c.Count)
			}
		}

		return nil
	},
}

func init() {
	geoCmd.AddCommand(geoStatusCmd)
}
