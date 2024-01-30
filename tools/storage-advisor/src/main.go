package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithy "github.com/aws/smithy-go"
	"github.com/fatih/color"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
)

type Config struct {
	Region          string `json:"region"`
	InventoryBucket string `json:"inventoryBucket"`
	Prefix          string `json:"prefix"`
}

var mode = flag.String("mode", "cli", "Use as a CLI tool")

var region = flag.String("region", "", "AWS region")

var apiToken = flag.String("api-token", "", "API token for Joom Cloud")

var apiEndpoint = flag.String("api-endpoint", "https://api.cloud.joom.ai/v1", "API endpoint URL")

func main() {
	flag.Parse()
	rootCtx := context.Background()
	cfg, err := config.LoadDefaultConfig(rootCtx)
	if err != nil {
		fmt.Printf("Error: could not initialize AWS config: %v\n", err)
		return
	}
	if region != nil {
		cfg.Region = *region
	}
	client := s3.NewFromConfig(cfg)
	api := NewApi(*apiEndpoint, apiToken)

	switch *mode {
	case "cli":
		// For CLI, use a simplified logger
		output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: ""}
		output.FormatTimestamp = func(i interface{}) string {
			return ""
		}
		output.FormatLevel = func(i interface{}) string {
			switch i.(string) {
			case "err":
				return "Error: "
			case "warn":
				return "Warning: "
			default:
				return ""
			}
		}
		log.Logger = zerolog.New(output).Level(zerolog.InfoLevel)

		if cfg.Region == "" {
			fmt.Printf("Error: AWS region is not set.\n\n" +
				"Please either configured default region with\n" +
				"    aws configure\n" +
				"or specify the region as option, e.g.\n" +
				"    storage-advisor -region eu-central-1\n" +
				"(adjusting to your actual region)\n")
			return
		}

		stsOutput, err := sts.NewFromConfig(cfg).GetCallerIdentity(rootCtx, &sts.GetCallerIdentityInput{})
		if err != nil {
			fmt.Printf("Error: unable to get your AWS indentity: %v\n\n"+
				"Possibly, you did not login to AWS or your token has expired.\n"+
				"Please try to login again. Then, verify your identity using\n\n"+
				"    aws sts get-caller-identity\n\n"+
				"If you are logged in, but still see this error, you might not\n"+
				"have permissions to list S3 bucket. Double-check that IAM role\n"+
				"associated with the role printed by the above command.\n\n", err)
			return
		}
		identity := *stsOutput.Arn
		fmt.Printf("Info: your AWS identity is %s\n", identity)

		buckets, issues, err := runS3Checks(rootCtx, client)
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) {
				color.Red("Could not list buckets: %s: %s", ae.ErrorCode(), ae.ErrorMessage())
				return
			} else {
				color.Red("Could not list buckets: %s", err.Error())
				return
			}
		}

		writeBasicIssues(issues, identity)

		if api.IsConfigured() {
			uploadBasicIssues(api, buckets)
			count := uploadInventories(api, client, buckets)
			fmt.Printf(green("Uploaded inventory for %d buckets\n", count))
		}
		return

	case "aws":
		if apiToken == nil {
			log.Fatal().Msg("api token is not provided; either provide it or run in CLI mode")
			return
		}

		handler := func(ctx context.Context) (events.APIGatewayProxyResponse, error) {

			stsOutput, err := sts.NewFromConfig(cfg).GetCallerIdentity(rootCtx, &sts.GetCallerIdentityInput{})
			if err != nil {
				log.Err(err).Msg("could not determine AWS identity, this usually means AWS auth is missing")
			} else {
				log.Info().Msgf("running with AWS identity %s", *stsOutput.Arn)
			}

			buckets, _, err := runS3Checks(rootCtx, client)
			if err != nil {
				log.Fatal().Err(err).Msg("could not list buckets")
			}

			uploadBasicIssues(api, buckets)

			count := uploadInventories(api, client, buckets)

			response := events.APIGatewayProxyResponse{
				StatusCode: 200,
				Body:       fmt.Sprintf("uploaded invetories for %d buckets", count),
			}

			return response, nil
		}

		lambda.Start(handler)
	}

}

func uploadInventories(api *JoomCloudAPI, client *s3.Client, buckets []Bucket) int {
	count := 0
	for _, bucket := range buckets {
		if bucket.UsableInventory == "" {
			continue
		}
		log.Info().Msgf("Processing inventory s3://%s", bucket.UsableInventory)
		i := strings.Index(bucket.UsableInventory, "/")
		inventoryBucket := bucket.UsableInventory[:i]
		inventoryPrefix := bucket.UsableInventory[i+1:]
		uploadInventoryForBucket(client, api, inventoryBucket, inventoryPrefix)
		count = count + 1
	}
	return count
}
