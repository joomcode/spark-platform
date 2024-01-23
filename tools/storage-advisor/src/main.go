package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"log"
	"os"
	"strings"
)

type Config struct {
	Region          string `json:"region"`
	InventoryBucket string `json:"inventoryBucket"`
	Prefix          string `json:"prefix"`
}

var mode = flag.String("mode", "cli", "Use as a CLI tool")
var region = flag.String("region", "", "AWS region to use")

var inventoryPrefix = flag.String("prefix", "", "Root inventory prefix")
var inventoryBucket = flag.String("inventory_bucket", "", "Bucket the inventory is stored in")
var apiToken = flag.String("api-token", "", "API token for Joom Cloud")

var apiEndpoint = flag.String("api-endpoint", "https://api.cloud.joom.ai/v1", "API endpoint URL")

func main() {
	flag.Parse()
	rootCtx := context.TODO()
	cfg, err := config.LoadDefaultConfig(rootCtx)
	if err != nil {
		fmt.Printf("Error: could not initialize AWS config: %v\n", err.Error())
		return
	}

	api := NewApi(*apiEndpoint, apiToken)

	switch *mode {
	case "cli":
		if *region != "" {
			cfg.Region = *region
		}

		stsOutput, err := sts.NewFromConfig(cfg).GetCallerIdentity(rootCtx, &sts.GetCallerIdentityInput{})
		if err != nil {
			fmt.Printf("Error: unable to get your AWS indentity\n\n")

			fmt.Printf(
				"\nPossibly, you did not login to AWS or your token has expired.\n" +
					"Please try to login again. Then, verify your identity using\n\n" +
					"    aws sts get-caller-identity\n\n" +
					"If you are logged in, but still see this error, you might not\n" +
					"have permissions to list S3 bucket. Double-check that IAM role\n" +
					"associated with the role printed by the above command.\n\n")

			return
		}
		identity := *stsOutput.Arn
		fmt.Printf("Info: your AWS identity is %s\n", identity)

		buckets := runS3Checks(cfg, api, identity)

		client := s3.NewFromConfig(cfg)

		for _, bucket := range buckets {
			if bucket.UsableInventory != "" {
				fmt.Printf("Trying to process inventory s3://%s\n", bucket.UsableInventory)
				i := strings.Index(bucket.UsableInventory, "/")
				inventoryBucket := bucket.UsableInventory[:i]
				inventoryPrefix := bucket.UsableInventory[i+1:]
				uploadInventoryForBucket(client, api, inventoryBucket, inventoryPrefix)
			}
		}
		return

	case "aws":
		log.Println("Running in AWS lambda mode")

		*inventoryPrefix = os.Getenv("prefix")
		*region = os.Getenv("region")
		*inventoryBucket = os.Getenv("inventoryBucket")
		*apiToken = os.Getenv("jwt")

		validateInputs()

		cfg.Region = *region

		client := s3.NewFromConfig(cfg)

		handler := func(ctx context.Context) (events.APIGatewayProxyResponse, error) {
			prefixList, err := listCommonPrefixes(client, *inventoryBucket, *inventoryPrefix)

			if err != nil {
				response := events.APIGatewayProxyResponse{
					StatusCode: 500,
					Body:       fmt.Sprintf("Failed to list prefix %s", err),
				}

				return response, err
			}

			for _, prefix := range prefixList {
				uploadInventoryForBucket(client, api, *inventoryBucket, prefix+"default")
			}

			response := events.APIGatewayProxyResponse{
				StatusCode: 200,
				Body:       fmt.Sprintf("Processed prefixes %s for bucket %s", prefixList, *inventoryBucket),
			}

			return response, nil
		}

		lambda.Start(handler)
	}

}

func validateInputs() {
	if *region == "" {
		log.Fatal("region is empty")
	}

	if *inventoryBucket == "" {
		log.Fatal("inventoryBucket is empty")
	}

	if *inventoryPrefix == "" {
		log.Fatal("prefix is empty")
	}

	if *apiToken == "" {
		log.Fatal("jwt token is empty")
	}

	log.Println("Processing", *inventoryPrefix, *inventoryBucket, *region)
}
