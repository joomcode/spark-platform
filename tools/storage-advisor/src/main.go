package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"log"
	"os"
	"strings"
)

type Config struct {
	Region          string   `json:"region"`
	InventoryBucket string   `json:"inventoryBucket"`
	Prefixes        []string `json:"prefixes"`
}

var cliMode = flag.String("mode", "cli", "Use as a CLI tool")
var region = flag.String("region", "", "AWS region to use")

var prefixes = flag.String("prefixes", "", "Comma separated list of inventory prefix")
var inventoryBucket = flag.String("inventory_bucket", "", "Bucket the inventory is stored in")
var authJwtToken = flag.String("jwt", "", "JWT authJwtToken from cloud")

//uploadInventoryForBucket(cfg, "joom-analytics-logs", "s3-inventory/joom-analytics-ads/default")

func main() {
	log.Println(os.Args)
	flag.Parse()
	rootCtx := context.TODO()
	cfg, err := config.LoadDefaultConfig(rootCtx)
	switch *cliMode {
	case "cli":
		log.Println("Running in CLI mode")

		cfg.Region = *region
		if err != nil {
			log.Println("Error:", err)
			return
		}

		runS3Checks(cfg, err)
		prefixList := strings.Split(*prefixes, ",")

		for _, prefix := range prefixList {
			uploadInventoryForBucket(cfg, *authJwtToken, *inventoryBucket, prefix)
		}
		return
	case "AWS":
		log.Println("Running in AWS lambda mode")

		handler := func(ctx context.Context) (events.APIGatewayProxyResponse, error) {
			jwt := os.Getenv("jwt")
			conf := Config{
				Prefixes:        strings.Split(os.Getenv("prefixes"), ","),
				Region:          os.Getenv("region"),
				InventoryBucket: os.Getenv("inventoryBucket"),
			}

			if conf.Region == "" {
				log.Fatal("region is empty")
			}

			if conf.InventoryBucket == "" {
				log.Fatal("inventoryBucket is empty")
			}

			if len(conf.Prefixes) == 0 {
				log.Fatal("prefixes are empty")
			}

			if jwt == "" {
				log.Fatal("jwt token is empty")
			}

			log.Printf("Processing '%s'\n", conf)

			for _, prefix := range conf.Prefixes {
				uploadInventoryForBucket(cfg, jwt, conf.InventoryBucket, prefix)
			}

			response := events.APIGatewayProxyResponse{
				StatusCode: 200,
				Body:       fmt.Sprintf("Processed prefixes %s for bucket %s", conf.Prefixes, conf.InventoryBucket),
			}

			return response, nil
		}

		lambda.Start(handler)
	}

}
