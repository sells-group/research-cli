package main

import (
	"fmt"
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
		fmt.Println("=== Geocoding Status ===")
		fmt.Printf("Total addresses:    %d\n", totalAddresses)
		fmt.Printf("Geocoded:           %d\n", geocoded)
		fmt.Printf("Ungeocoded:         %d\n", ungeocoded)
		fmt.Printf("MSA associations:   %d\n", msaAssociations)
		fmt.Println()

		if len(sources) > 0 {
			fmt.Println("Geocode sources:")
			for _, s := range sources {
				fmt.Printf("  %-15s %d\n", s.Source, s.Count)
			}
			fmt.Println()
		}

		if len(classifications) > 0 {
			fmt.Println("Classification breakdown:")
			for _, c := range classifications {
				fmt.Printf("  %-15s %d\n", c.Classification, c.Count)
			}
		}

		return nil
	},
}

func init() {
	geoCmd.AddCommand(geoStatusCmd)
}
