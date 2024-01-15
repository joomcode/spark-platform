package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func uploadInventoryForBucket(client *s3.Client, jwtToken string, bucket string, prefix string) {

	datePrefixies, err := listCommonPrefixes(client, bucket, prefix)
	dates := make([]time.Time, 0, 20)

	dateFormat := "2006-01-02T15-04Z"
	for _, datePrefix := range datePrefixies {
		dateStr := strings.TrimPrefix(datePrefix, prefix)
		dateStr = strings.TrimPrefix(dateStr, "/")
		dateStr = strings.TrimSuffix(dateStr, "/")
		tailAsDate, err := time.Parse("2006-01-02T15-04Z", dateStr)
		if err == nil {
			dates = append(dates, tailAsDate)
		} else {
			log.Println("Error: Skipping unexpected non-date prefix", datePrefix)
		}
	}
	if len(dates) == 0 {
		log.Println("Error: No dates found")
		return
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

func listCommonPrefixes(client *s3.Client, bucket string, prefix string) ([]string, error) {

	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	log.Println("Listing prefix", bucket, prefix)

	paginator := s3.NewListObjectsV2Paginator(client, params)
	var commonPrefixes []string
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Println("Error:", err)
			return nil, err
		}
		for _, commonPrefix := range output.CommonPrefixes {
			commonPrefixes = append(commonPrefixes, *commonPrefix.Prefix)
		}
	}

	return commonPrefixes, nil
}
