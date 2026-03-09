package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"datascope-cli/pkg/datascope"

	"github.com/urfave/cli/v3"
)

func readLines(fn string) ([]string, error) {
	var lines []string
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func parseIdentifiers(args []string) ([]string, error) {
	var identifiers []string
	for _, arg := range args {
		if strings.HasPrefix(arg, "@") {
			lines, err := readLines(arg[1:])
			if err != nil {
				return nil, err
			}
			identifiers = append(identifiers, lines...)
		} else {
			identifiers = append(identifiers, arg)
		}
	}
	if len(identifiers) == 0 {
		return nil, fmt.Errorf("no idenfifiers provided")
	}
	return identifiers, nil
}

func parseFields(arg string, fieldSet map[string][]string) ([]string, error) {
	fields, ok := fieldSet[arg]
	if !ok && arg != "" {
		if strings.HasPrefix(arg, "@") {
			lines, err := readLines(arg[1:])
			if err != nil {
				return nil, err
			}
			fields = lines
		} else if strings.HasPrefix(arg, "+") {
			fields = append(fieldSet["DEFAULT"], strings.Split(arg[1:], ",")...)
		} else {
			fields = strings.Split(arg, ",")
		}
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
	}
	if len(fields) == 0 {
		fields = fieldSet["DEFAULT"]

	}
	return fields, nil
}

func printExtractionResponseCSV(resp *datascope.ExtractionResponse, fields []string, cmd *cli.Command) error {
	if len(resp.Contents) == 0 {
		return nil
	}

	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	headers := fields
	if len(headers) == 0 {
		for k := range resp.Contents[0] {
			headers = append(headers, k)
		}
		sort.Strings(headers)
	}

	if err := w.Write(headers); err != nil {
		return err
	}

	for _, row := range resp.Contents {
		var line []string
		for _, h := range headers {
			v := row[h]
			if v == nil {
				line = append(line, "")
			} else {
				line = append(line, fmt.Sprint(v))
			}
		}
		if err := w.Write(line); err != nil {
			return err
		}
	}

	return nil
}

func printExtractionResponse(resp *datascope.ExtractionResponse, fields []string, cmd *cli.Command) error {
	if cmd.Bool("csv") {
		return printExtractionResponseCSV(resp, fields, cmd)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp.Contents)

	if cmd.Bool("verbose") {
		fmt.Println("\nNOTES\n" + strings.Join(resp.Notes, "\n"))
	}
	return nil
}

func main() {
	var c *datascope.Client

	cmd := &cli.Command{
		Name:  "datascope-cli",
		Usage: "CLI for Refinitiv DataScope Select API",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Enable debug logging",
			},
			&cli.BoolFlag{
				Name:  "csv",
				Usage: "Output as CSV",
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Usage:   "Verbose mode",
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			username := os.Getenv("DATASCOPE_USERNAME")
			password := os.Getenv("DATASCOPE_PASSWORD")

			if username == "" || password == "" {
				return ctx, cli.Exit("username and password must be provided via environment variables (DATASCOPE_USERNAME, DATASCOPE_PASSWORD)", 2)
			}

			c = datascope.NewClient(username, password)
			c.Debug = cmd.Bool("debug")
			return ctx, nil
		},
		Commands: []*cli.Command{
			{
				Name:  "extract",
				Usage: "Extract instrument",
				Commands: []*cli.Command{
					{
						Name:    "endofday",
						Usage:   "Extract end of day",
						Aliases: []string{"eod"},
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  "fields",
								Value: "DEFAULT",
								Usage: "Fields to extract (DEFAULT, ALL, or [+]comma-separated list)",
							},
						},
						Action: func(ctx context.Context, cmd *cli.Command) error {
							fields, err := parseFields(cmd.String("fields"), datascope.EndOfDayFields)
							if err != nil {
								return err
							}

							args, err := parseIdentifiers(cmd.Args().Slice())
							if err != nil {
								return err
							}

							resp, err := c.ExtractEndOfDay(args, fields)
							if err != nil {
								return err
							}

							return printExtractionResponse(resp, fields, cmd)
						},
					},
					{
						Name:    "composite",
						Usage:   "Extract composite",
						Aliases: []string{"comp"},
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  "fields",
								Value: "DEFAULT",
								Usage: "Fields to extract (DEFAULT, ALL, or [+]comma-separated list)",
							},
						},
						Action: func(ctx context.Context, cmd *cli.Command) error {
							fields, err := parseFields(cmd.String("fields"), datascope.CompositeFields)
							if err != nil {
								return err
							}

							args, err := parseIdentifiers(cmd.Args().Slice())
							if err != nil {
								return err
							}

							resp, err := c.ExtractComposite(args, fields)
							if err != nil {
								return err
							}

							return printExtractionResponse(resp, fields, cmd)
						},
					},
					{
						Name:    "pricehistory",
						Usage:   "Extract price history",
						Aliases: []string{"ph"},
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:  "date",
								Usage: "Date (YYYY-MM-DD)",
							},
							&cli.IntFlag{
								Name:  "days",
								Usage: "Number of days of history",
								Value: 20,
							},
							&cli.StringFlag{
								Name:  "start-date",
								Usage: "Start date (YYYY-MM-DD)",
							},
							&cli.StringFlag{
								Name:  "end-date",
								Usage: "End date (YYYY-MM-DD)",
							},
						},
						Action: func(ctx context.Context, cmd *cli.Command) error {
							fields, err := parseFields(cmd.String("fields"), datascope.PriceHistoryFields)
							if err != nil {
								return err
							}

							identifiers, err := parseIdentifiers(cmd.Args().Slice())
							if err != nil {
								return err
							}
							startDate := cmd.String("start-date")
							endDate := cmd.String("end-date")
							if cmd.IsSet("date") {
								startDate = cmd.String("date")
								endDate = startDate
							}
							resp, err := c.ExtractPriceHistory(identifiers, fields, cmd.Int("days"), startDate, endDate)
							if err != nil {
								return err
							}
							return printExtractionResponse(resp, fields, cmd)
						},
					},
					{
						Name:  "intraday",
						Usage: "Extract intraday pricing",
						Action: func(ctx context.Context, cmd *cli.Command) error {
							fields := datascope.IntradayFields["DEFAULT"]

							identifiers, err := parseIdentifiers(cmd.Args().Slice())
							if err != nil {
								return err
							}

							resp, err := c.ExtractIntraday(identifiers, fields)
							if err != nil {
								return err
							}
							return printExtractionResponse(resp, fields, cmd)
						},
					},
					{
						Name:    "corporateactions",
						Usage:   "Extract corporate actions",
						Aliases: []string{"ca"},
						Action: func(ctx context.Context, cmd *cli.Command) error {
							fields := datascope.CorporateActionsFields["DEFAULT"]

							identifiers, err := parseIdentifiers(cmd.Args().Slice())
							if err != nil {
								return err
							}

							resp, err := c.ExtractCorporateActions(identifiers, fields)
							if err != nil {
								return err
							}
							return printExtractionResponse(resp, fields, cmd)
						},
					},
				},
			},
			{
				Name:  "search",
				Usage: "Search instrument",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					args, err := parseIdentifiers(cmd.Args().Slice())
					if err != nil {
						return err
					}

					resp, err := c.Search(args[0])
					if err != nil {
						return err
					}

					b, _ := json.MarshalIndent(resp.Value, "", "  ")
					fmt.Println(string(b))

					return nil
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if exitErr, ok := err.(cli.ExitCoder); ok {
			os.Exit(exitErr.ExitCode())
		}
		os.Exit(1)
	}
}
