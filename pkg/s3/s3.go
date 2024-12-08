package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const logRateLimiterTokens = false

type S3Obliterator struct {
	checkpoint        *CheckpointMetadata
	workers           int
	bucket            string
	prefixRateLimiter *PrefixRateLimiter
	service           *s3.S3
}

type S3RemovalJob struct {
	Region string
	RawKey string // The raw key from the key file
	Key    string // The normalized key which helps prevent inadvertently removing too many things
	Prefix bool
}

type S3RemovalJobResult struct {
	workerId string
	job      S3RemovalJob
	err      error
}

func NewS3Obliterator(checkpointFilename string, region string, bucket string, workers int, rateLimit int) (*S3Obliterator, error) {
	svc, errSvc := GetS3Service(region)
	if errSvc != nil {
		return nil, fmt.Errorf("failed to get S3 service: %v", errSvc)
	}

	if errVersioning := checkBucketVersioning(svc, bucket); errVersioning != nil {
		return nil, errVersioning
	}

	return &S3Obliterator{
		// checkpointFilename: checkpointFilename,
		checkpoint:        NewCheckpointMetadata(checkpointFilename),
		bucket:            bucket,
		workers:           workers,
		service:           svc,
		prefixRateLimiter: NewPrefixRateLimiter(rateLimit, 3*rateLimit),
	}, nil
}

// At present, versioning is not handled by the obliterator - so we just detect it and cancel the process
func checkBucketVersioning(svc *s3.S3, bucket string) error {
	result, errGet := svc.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if errGet != nil {
		return fmt.Errorf("failed to get bucket versioning: %v", errGet)
	}

	// Versioning is disabled
	if result.Status == nil {
		return nil
	}

	if aws.StringValue(result.Status) == s3.BucketVersioningStatusSuspended {
		return nil
	}

	return fmt.Errorf("versioning is enabled on bucket %s", bucket)
}

func GetS3Service(region string) (*s3.S3, error) {
	sess, err := session.NewSession(aws.NewConfig().WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %v", err)
	}

	svc := s3.New(sess)
	return svc, nil
}

// Given a prefix of /a/, and a key of /a/beta/gamma, return beta
// Given a prefix of /a/b, and a key of /a/beta/gamma, return beta
func GetLoggableKey(prefix string, key string, length int) string {
	// We assume that key is prefixed with prefix, or the fabric of the universe has broken down
	if !strings.HasPrefix(key, prefix) {
		panic("key does not start with prefix")
	}
	prefixCount := len(strings.Split(prefix, "/"))
	keyPieces := strings.Split(key, "/")

	loggableKey := strings.Join(keyPieces[prefixCount-1:prefixCount], "/")
	if length == 0 || length >= len(loggableKey) {
		return loggableKey
	}
	return loggableKey[:length]
}

func (s3o *S3Obliterator) Execute(ctx context.Context, jobs []S3RemovalJob) error {
	errLoad := s3o.checkpoint.Load()
	if errLoad != nil {
		return errLoad
	}

	logrus.Infof("Removal jobs requested: %d", len(jobs))

	// There is an argument for moving this to the caller and passing in the checkpointer, but this will do for now
	// using the checkpoint file, filter out jobs that have already completed
	validJobs := make([]S3RemovalJob, 0)
	for _, job := range jobs {
		if !s3o.checkpoint.IsComplete(job.RawKey) {
			validJobs = append(validJobs, job)
		}
	}
	jobs = validJobs

	if len(jobs) == 0 {
		logrus.Info("No jobs to be completed")
		return nil
	}

	logrus.Infof("Removal jobs to be completed: %d", len(jobs))

	jobChan := make(chan S3RemovalJob, len(jobs))
	jobResultChan := make(chan S3RemovalJobResult, len(jobs))

	// Worker goroutines
	for workerId := 0; workerId < s3o.workers; workerId++ {
		go s3o.worker(ctx, fmt.Sprintf("%02d", workerId), jobChan, jobResultChan)
	}

	// Generator: send jobs to the workers
	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)

	done := 0
	for {
		select {
		case result, ok := <-jobResultChan:
			if !ok {
				return nil
			}
			logger := logrus.
				WithField("bucket", s3o.bucket).
				WithField("key", result.job.Key)

			if result.err != nil {
				logger.Warnf("Failure removing key Error: %v", result.err)
				return fmt.Errorf("failed to remove key: %v: %v", result.job, result.err)
			}

			done++
			logger.Infof("Success removing key (%d remaining)", len(jobs)-done)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s3o *S3Obliterator) worker(ctx context.Context, workerId string, jobs <-chan S3RemovalJob, jobResults chan<- S3RemovalJobResult) {
	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				return
			}

			s3o.checkpoint.Start(job.RawKey)

			logger := logrus.New().
				WithField("workerId", workerId).
				WithField("bucket", s3o.bucket).
				WithField("key", job.Key)

			errRemove := s3o.removeKeyWithRetry(ctx, logger, job.Key)
			if errRemove == nil {
				// We only mark things as finished if they didn't error - this way they will restart again later
				s3o.checkpoint.Finish(job.RawKey)
				s3o.checkpoint.Save()
			}

			// Here you can add the code to process the job
			// After processing, send the job back as a result
			jobResults <- S3RemovalJobResult{
				workerId: workerId,
				job:      job,
				err:      errRemove,
			}
		case <-ctx.Done(): // closes when the caller cancels the ctx
			jobResults <- S3RemovalJobResult{
				err: ctx.Err(),
			}
			return
		}
	}
}

func (s3o *S3Obliterator) removeKeyWithRetry(ctx context.Context, logger *logrus.Entry, key string) error {
	const maxFailures = 3
	failures := 0
	for {
		errRemove := s3o.removeKey(ctx, logger, key)
		if errRemove == nil {
			return nil
		}

		// If we got a slow down warning, then lets have a little nap
		if strings.Contains(errRemove.Error(), "SlowDown") || strings.Contains(errRemove.Error(), "TooManyRequestsException") {
			limiter := s3o.prefixRateLimiter.GetRateLimiter(key)

			logger.WithFields(withRateLimiterTokens(limiter)).Warnf("Rate limited by AWS")
			// Consume our burst rate limit of tokens to slow down the rate of requests across all workers
			limiter.WaitN(ctx, limiter.Burst())
			continue
		}

		// The subtlety here is that you can be rate limited, but it doesn't count to your failure limit
		failures++
		if failures < maxFailures {
			logger.Warnf("Retrying (%d/%d) after failure to remove key: %v", failures, maxFailures, errRemove)
			// Just give some time and space for the system to recover before retrying
			time.Sleep(5 * time.Second)
			continue

		}
		logger.Infof("Failed to remove key after %d attempts", maxFailures)
		return errRemove
	}
}

// withRateLimiterTokens adds the rate limiter tokens to the fields if logRateLimiterTokens is true
// Most of the time we're not interested in the tokens, so we don't want to log them
func withRateLimiterTokens(limiter *rate.Limiter) map[string]any {
	if logRateLimiterTokens {
		return map[string]any{"limiter.tokens": limiter.Tokens()}
	}
	return nil
}

func (s3o *S3Obliterator) removeKey(ctx context.Context, logger *logrus.Entry, key string) error {
	var continuationToken *string
	for {
		if err := ctx.Err(); err != nil {
			logger.Info("Removal cancelled by context")
			return ctx.Err()
		}

		// We don't rate limit the GET / List as it is unlikely we can hit this with the rate limiting on deletes

		listObjectsResult, errList := s3o.service.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(s3o.bucket),
			Prefix:            aws.String(key),
			ContinuationToken: continuationToken,
		})
		continuationToken = listObjectsResult.NextContinuationToken
		if errList != nil {
			return fmt.Errorf("failed to list objects from bucket %s: %v", s3o.bucket, errList)
		}

		if len(listObjectsResult.Contents) == 0 {
			logger.Debugf("No objects found")
			break
		}

		loggableStartKey := GetLoggableKey(key, *listObjectsResult.Contents[0].Key, 8)
		loggableEndKey := GetLoggableKey(key, *listObjectsResult.Contents[len(listObjectsResult.Contents)-1].Key, 8)

		if *listObjectsResult.IsTruncated {
			// There are more objects, so show some detail on key names so far
			logger.Debugf("Found %4d matching objects (%s ... %s)", len(listObjectsResult.Contents), loggableStartKey, loggableEndKey)
		} else {
			logger.Debugf("Found %4d matching objects", len(listObjectsResult.Contents))
		}

		objects := make([]*s3.ObjectIdentifier, len(listObjectsResult.Contents))
		for i, obj := range listObjectsResult.Contents {
			objects[i] = &s3.ObjectIdentifier{Key: obj.Key}
		}

		limiter := s3o.prefixRateLimiter.GetRateLimiter(key)

		// As long as we don't have too much parallelism - this is effective
		limiter.WaitN(ctx, len(objects))

		logger.WithFields(withRateLimiterTokens(limiter)).
			Infof("Sending S3 request to delete %4d objects (%s ... %s)", len(objects), loggableStartKey, loggableEndKey)
		_, errDelete := s3o.service.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s3o.bucket),
			Delete: &s3.Delete{
				Objects: objects,
			},
		})
		if errDelete != nil {
			return fmt.Errorf("failed to delete objects from bucket %s: %v", s3o.bucket, errDelete)
		}

		if !*listObjectsResult.IsTruncated {
			break
		}

	}

	return nil
}
