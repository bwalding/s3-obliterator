package removekeys

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bwalding/s3-obliterator/pkg/aws"
	"github.com/bwalding/s3-obliterator/pkg/s3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type removeKeysOptions struct {
	region    string
	bucket    string
	keyFile   string
	workers   int
	rateLimit int
}

func (o *removeKeysOptions) Configure(cmd *cobra.Command) error {
	cmd.Flags().StringVarP(&o.bucket, "bucket", "", "", "Name of the bucket to remove keys from")
	_ = cmd.MarkFlagRequired("bucket")
	cmd.Flags().StringVarP(&o.keyFile, "key-file", "", "", "Path to the file containing the keys to remove")
	_ = cmd.MarkFlagRequired("key-file")
	cmd.Flags().IntVarP(&o.workers, "workers", "", 10, "Number of workers to use")
	cmd.Flags().IntVarP(&o.rateLimit, "rate-limit", "", 3500, "Rate limit for S3 API calls")
	return nil
}

func CreateCommand() *cobra.Command {
	rko := &removeKeysOptions{}
	cmd := &cobra.Command{
		Use:   "remove-keys",
		Short: "Remove keys from a bucket",
		RunE: func(cmd *cobra.Command, args []string) error {

			if err := aws.CheckAWSCredentials(); err != nil {
				return fmt.Errorf("failed to check AWS credentials: %v", err)
			}

			logrus.Infof("Removing keys from bucket %s", rko.bucket)
			logrus.Infof("Using key file %s", rko.keyFile)
			logrus.Infof("Using %d workers", rko.workers)
			logrus.Infof("Rate limiting to %d", rko.rateLimit)

			// Create a cancellable context
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Set up a channel to listen for interrupt signals
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT)
			go func() {
				<-sigChan
				logrus.Warn("Received SIGINT, cancelling obliteration...")
				cancel() // Cancel the context on SIGINT
			}()

			return Execute(ctx, rko)
		},
	}

	rko.Configure(cmd)

	return cmd
}

func Execute(ctx context.Context, rko *removeKeysOptions) error {
	// Read the key file to get all the prefixes
	// Read the checkpoint file to find out which ones are complete
	// Launch a parallel processing engine to remove the keys
	// AWS rate limits based on a variety

	// Read the rko.keyfile into a list of lines and remove trailing newlines
	data, err := os.ReadFile(rko.keyFile)
	if err != nil {
		return err
	}

	rawLines := strings.Split(string(data), "\n")
	var jobs []s3.S3RemovalJob
	for _, rawLine := range rawLines {
		rawKey, _, _ := strings.Cut(rawLine, "#")
		rawLine = strings.TrimSpace(rawKey)
		if rawKey == "" {
			continue
		}

		jobs = append(jobs, s3.S3RemovalJob{
			Region: rko.region,
			RawKey: rawKey,
			Key:    NormalizeKey(rawLine),
			Prefix: true,
		})
	}
	logrus.Infof("Read %d keys from %s", len(jobs), rko.keyFile)

	// For each key in the key file, generate a removal job
	// The generator can then spin off extra jobs to break work down etc
	s3o, errNew := s3.NewS3Obliterator(rko.keyFile+".checkpoint", rko.region, rko.bucket, rko.workers, rko.rateLimit)
	if errNew != nil {
		return fmt.Errorf("failed to create S3Obliterator: %v", errNew)
	}
	return s3o.Execute(ctx, jobs)
}

func NormalizeKey(key string) string {
	key = strings.TrimSpace(key)

	// lines that end with * are permitted verbatim (with the * removed)
	if strings.HasSuffix(key, "*") {
		return strings.TrimSuffix(key, "*")
	}

	// lines that end with / are permitted verbatim (with the / removed)
	if strings.HasSuffix(key, "/") {
		return key
	}

	logrus.Debugf("Appending / to %s", key)
	return key + "/"
}
