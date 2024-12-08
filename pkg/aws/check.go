package aws

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/sirupsen/logrus"
)

func CheckAWSCredentials() error {

	sess, err := session.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create session: %v", err)
	}

	svc := sts.New(sess)
	input := &sts.GetCallerIdentityInput{}

	result, err := svc.GetCallerIdentity(input)
	if err != nil {
		// We do not want to just assume they want this turned on.
		if os.Getenv("AWS_SDK_LOAD_CONFIG") == "" {
			logrus.Warn("AWS_SDK_LOAD_CONFIG is not set. This may cause issues with the AWS SDK. Please set AWS_SDK_LOAD_CONFIG=1")
		}

		return fmt.Errorf("failed to get caller identity: %v", err)
	}

	logrus.Infof("AWS credentials are correctly configured for user: %v\n", *result.Arn)
	return nil
}
