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
var authJwtToken = flag.String("jwt", "", "JWT authJwtToken from cloud")

func main() {
	log.Println(os.Args)
	flag.Parse()
	rootCtx := context.TODO()
	cfg, err := config.LoadDefaultConfig(rootCtx)
	switch *mode {
	case "cli":
		log.Println("Running in CLI mode")

		cfg.Region = *region
		if err != nil {
			log.Println("Error:", err)
			return
		}

		//runS3Checks(cfg, err)
		prefixList, err := findPrefixes(cfg, *inventoryBucket, *inventoryPrefix)

		if err != nil {
			log.Fatal("Failed to list prefix", err)
		}

		for _, prefix := range prefixList {
			uploadInventoryForBucket(cfg, *authJwtToken, *inventoryBucket, prefix+"default")
		}
		return
	case "aws":
		log.Println("Running in AWS lambda mode")

		handler := func(ctx context.Context) (events.APIGatewayProxyResponse, error) {
			jwt := os.Getenv("jwt")
			conf := Config{
				Prefix:          os.Getenv("prefix"),
				Region:          os.Getenv("region"),
				InventoryBucket: os.Getenv("inventoryBucket"),
			}

			if conf.Region == "" {
				log.Fatal("region is empty")
			}

			if conf.InventoryBucket == "" {
				log.Fatal("inventoryBucket is empty")
			}

			if conf.Prefix == "" {
				log.Fatal("prefix is empty")
			}

			if jwt == "" {
				log.Fatal("jwt token is empty")
			}

			log.Printf("Processing '%s'\n", conf)

			prefixList, err := findPrefixes(cfg, *inventoryBucket, *inventoryPrefix)

			if err != nil {
				response := events.APIGatewayProxyResponse{
					StatusCode: 500,
					Body:       fmt.Sprintf("Failed to list prefix %s", err),
				}

				return response, err
			}

			for _, prefix := range prefixList {
				uploadInventoryForBucket(cfg, jwt, conf.InventoryBucket, prefix)
			}

			response := events.APIGatewayProxyResponse{
				StatusCode: 200,
				Body:       fmt.Sprintf("Processed prefixes %s for bucket %s", prefixList, conf.InventoryBucket),
			}

			return response, nil
		}

		lambda.Start(handler)
	}

}
