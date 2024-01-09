package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

func readObjectContent(client *s3.Client, bucket string, key string) ([]byte, error) {
	r, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func presignFile(presignClient *s3.PresignClient, bucket string, fileKey string) string {
	presignGetObject, err := presignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String(fileKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		panic(err)
	}

	return presignGetObject.URL
}

func uploadInventoryForBucket(cfg aws.Config, jwtToken string, bucket string, prefix string) {
	client := s3.NewFromConfig(cfg)

	// Enumerate objects in the bucket at the given prefix
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, input)
	var objects []types.Object
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Fatal("Error:", err)
			return
		}
		objects = append(objects, output.Contents...)
	}
	log.Printf("Found %d objects\n", len(objects))

	dates := make([]time.Time, 0, 20)
	dateFormat := "2006-01-02T15-04Z"
	for _, o := range objects {
		key := *o.Key
		// get key substring after prefix
		tail := key[len(prefix)+1:]
		next_slash := strings.Index(tail, "/")
		if next_slash != -1 {
			tail = tail[:next_slash]
			if strings.HasPrefix(tail, "20") {
				// parse tail as ISO date
				tailAsDate, err := time.Parse("2006-01-02T15-04Z", tail)
				if err == nil {
					dates = append(dates, tailAsDate)
				}
			}
		}
	}
	// Find max value in dates
	maxDate := dates[0]
	for _, d := range dates {
		if d.After(maxDate) {
			maxDate = d
		}
	}

	log.Printf("Max date: %s\n", maxDate.Format(time.DateTime))
	partitionDate := maxDate.Format(dateFormat)
	content, err := readObjectContent(client, bucket, prefix+"/"+partitionDate+"/manifest.json")
	if err != nil {
		log.Printf(err.Error())
	}

	var inputManifest InventoryManifest
	err = json.Unmarshal(content, &inputManifest)
	if err != nil {
		log.Fatalf("Could not unmarsal inputManifest: %s\n", err.Error())
	}
	log.Printf("Manifest has %d files for bucket %s\n", len(inputManifest.Files), inputManifest.Bucket)

	presignClient := s3.NewPresignClient(client)
	outputManifest := InventoryManifest{PartitionDate: partitionDate, Version: manifestVersion, Bucket: inputManifest.Bucket}

	for _, file := range inputManifest.Files {
		presignedUrl := presignFile(presignClient, bucket, file.Key)

		outputManifest.Files = append(outputManifest.Files, InventoryFile{
			Url:         presignedUrl,
			MD5checksum: file.MD5checksum,
		})
	}

	marshal, err := json.Marshal([]InventoryManifest{outputManifest})
	if err != nil {
		panic(err)
	}

	r, err := http.NewRequest("POST", "https://api.cloud.joom.ai/v1/sparkperformance/s3Inventory", bytes.NewBuffer(marshal))
	if err != nil {
		panic(err)
	}

	r.Header.Add("Authorization", "Bearer "+jwtToken)
	httpClient := &http.Client{}

	res, err := httpClient.Do(r)
	if err != nil {
		panic(err)
	}

	defer res.Body.Close()
	if res.StatusCode != 200 {
		body, err := ioutil.ReadAll(res.Body)
		var message string
		if err != nil {
			message = "Returned non-200 status"
		}

		message = fmt.Sprintf("Returned non-200 status '%d': %s", res.StatusCode, string(body))
		panic(message)
	}

	log.Printf("Uploaded data for s3://%s/%s", bucket, prefix)
}

func findPrefixes(cfg aws.Config, bucket string, prefix string) ([]string, error) {
	client := s3.NewFromConfig(cfg)

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	paginator := s3.NewListObjectsV2Paginator(client, params)
	var commonPrefixes []string
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Println("Error:", err)
			return nil, err
		}
		for _, commonPrefix := range output.CommonPrefixes {
			log.Println("Found prefix", *commonPrefix.Prefix)
			commonPrefixes = append(commonPrefixes, *commonPrefix.Prefix)
		}
	}

	return commonPrefixes, nil
}
