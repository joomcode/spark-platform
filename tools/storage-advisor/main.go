package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

type IssueWithCount struct {
	Issue string
	Count int
}

type Finding struct {
	Summary string
	Issue   string
}

func listS3Buckets(client *s3.Client) ([]string, error) {

	// Call ListBuckets to get a list of all buckets.
	result, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	// Extract the bucket names from the result.
	var bucketNames []string
	for _, bucket := range result.Buckets {
		bucketNames = append(bucketNames, *bucket.Name)
	}

	return bucketNames, nil
}

type Bucket struct {
	Name            string `json:"name"`
	Region          string `json:"region"`
	Ownership       string `json:"ownership"`
	OwnershipIssue  string `json:"ownershipIssue"`
	Versioning      string `json:"versioning"`
	VersioningIssue string `json:"versioningIssue"`
	Inventory       string `json:"inventory"`
	Logging         string `json:"logging"`
	Encryption      string `json:"encryption"`
	EncryptionIssue string `json:"encryptionIssue"`
}

// Given bucketname, returns AWS ownership rule. Ideally,
func getOwnership(client *s3.Client, bucket string) Finding {
	r, err := client.GetBucketOwnershipControls(context.TODO(), &s3.GetBucketOwnershipControlsInput{Bucket: &bucket})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "OwnershipControlsNotFoundError" {
				return Finding{"Not set", "No bucket ownership control settings"}
			} else {
				return Finding{"Error", "Error: " + ae.ErrorCode()}
			}
		} else {
			return Finding{"Error", "Error: " + err.Error()}
		}
	} else {
		if len(r.OwnershipControls.Rules) == 0 {
			return Finding{"Not set", "No bucket ownership control settings"}
		}
		if len(r.OwnershipControls.Rules) > 1 {
			return Finding{"Multiple", "Unexpected multiple ownership rules"}
		}
		ownership := r.OwnershipControls.Rules[0].ObjectOwnership
		issue := ""
		if ownership != types.ObjectOwnershipBucketOwnerEnforced {
			issue = "Bucket ownership not enforced"
		}
		return Finding{string(ownership), issue}
	}
}

// This function makes a HTTP POSt request with the given payload. The result is
// then checked. If HTTP status is not 2xx we return error.
func sendToAPI(payload []byte) error {
	requestURL := "http://localhost:3027/api/storage-advisor/buckets"
	req, err := http.NewRequest(http.MethodPost, requestURL, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Project-Id", "default")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func checkInventory(buckets []Bucket, client *s3.Client) {
	for i := range buckets {
		var inventory string

		r, err := client.ListBucketInventoryConfigurations(context.TODO(), &s3.ListBucketInventoryConfigurationsInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) {
				if ae.ErrorCode() == "InventoryConfigurationNotFoundError" {
					inventory = "No inventory configuration"
				} else {

					log.Printf("code: %s, message: %s, fault: %s", ae.ErrorCode(), ae.ErrorMessage(), ae.ErrorFault().String())
					inventory = ae.ErrorCode()
				}
			} else {
				log.Printf("Error: %f")
				inventory = err.Error()
			}
		} else {
			inventory = ""
			for _, c := range r.InventoryConfigurationList {
				location, _ := checkInventoryConfiguration(c)
				if location != "" {
					inventory = location
					break
				}
			}
			if inventory == "" {
				inventory = "No inventory configuration"
			}
		}
		fmt.Printf("Bucket %s: %s\n", buckets[i].Name, inventory)
		buckets[i].Inventory = inventory
	}

	bucketsWithNoInventory := 0
	for i := range buckets {
		if buckets[i].Inventory == "No inventory configuration" {
			bucketsWithNoInventory++
		}
	}
	if bucketsWithNoInventory > 0 {
		fmt.Printf("Bucket Inventory: %d buckets have no inventory configuration\n", bucketsWithNoInventory)
	} else {
		fmt.Printf("Bucket Inventory: all good\n")
	}
}

// Check if inventory configuration is OK
// If OK, returns the S3 location in the first value
// If not OK, returns all the issues in the second value
func checkInventoryConfiguration(c types.InventoryConfiguration) (string, []string) {
	issues := make([]string, 0)
	if !*c.IsEnabled {
		issues = append(issues, "not enabled")
	}
	if c.Destination.S3BucketDestination.Format != types.InventoryFormatParquet {
		issues = append(issues, "not in Parquet format")
	}
	if c.Schedule.Frequency != types.InventoryFrequencyDaily {
		issues = append(issues, "frequency is not daily")
	}
	if c.IncludedObjectVersions != types.InventoryIncludedObjectVersionsAll {
		issues = append(issues, "inventory includes only current versions")
	}
	if c.Filter != nil {
		issues = append(issues, "inventory has filter")
	}

	haveSize := false
	haveLastModified := false
	haveStorageClass := false
	haveIntelligentTieringAccessTier := false
	for _, f := range c.OptionalFields {
		switch f {
		case types.InventoryOptionalFieldSize:
			haveSize = true
		case types.InventoryOptionalFieldLastModifiedDate:
			haveLastModified = true
		case types.InventoryOptionalFieldStorageClass:
			haveStorageClass = true
		case types.InventoryOptionalFieldIntelligentTieringAccessTier:
			haveIntelligentTieringAccessTier = true
		default:
		}
	}
	if !haveSize || !haveLastModified || !haveStorageClass || !haveIntelligentTieringAccessTier {
		missing := make([]string, 0, 4)
		if !haveSize {
			missing = append(missing, "size")
		}
		if !haveLastModified {
			missing = append(missing, "last modified")
		}
		if !haveStorageClass {
			missing = append(missing, "storage class")
		}
		if !haveIntelligentTieringAccessTier {
			missing = append(missing, "intelligent tiering access tier")
		}
		issues = append(issues, fmt.Sprintf("missing fields: %s", strings.Join(missing, ", ")))
	}

	if len(issues) > 0 {
		return "", issues
	}

	return *c.Destination.S3BucketDestination.Bucket + "/" + *c.Destination.S3BucketDestination.Prefix, []string{}
}

func checkVersioning(buckets []Bucket, client *s3.Client) {
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		var finding Finding
		r, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = Finding{"Error", err.Error()}
		} else {
			status := string(r.Status)
			if status == "" {
				status = "Not set"
			}
			issue := ""
			if r.Status != types.BucketVersioningStatusEnabled {
				issue = "Versioning not enabled"
			}
			finding = Finding{status, issue}
		}
		results = append(results, finding)
		buckets[i].Versioning = finding.Summary
		buckets[i].VersioningIssue = finding.Issue
	}

	summarizeIssues(results, "Bucket Versioning")
}

func checkEncryption(buckets []Bucket, client *s3.Client) {
	results := make([]Finding, 0, len(buckets))

	for i := range buckets {
		var finding Finding
		// Get bucket encryption
		r, err := client.GetBucketEncryption(context.TODO(), &s3.GetBucketEncryptionInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = Finding{"Error", err.Error()}
		} else {
			if len(r.ServerSideEncryptionConfiguration.Rules) == 0 {
				finding = Finding{"Not set", "No bucket encryption settings"}
			} else if len(r.ServerSideEncryptionConfiguration.Rules) > 1 {
				finding = Finding{"Multiple", "Unexpected multiple encryption rules"}
			} else {
				rule := r.ServerSideEncryptionConfiguration.Rules[0]
				if rule.ApplyServerSideEncryptionByDefault == nil {
					finding = Finding{"Not set", "No bucket encryption settings"}
				} else {
					issue := ""
					if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != types.ServerSideEncryptionAes256 {
						if rule.BucketKeyEnabled == nil || !*rule.BucketKeyEnabled {
							issue = "KMS encryption without bucket key"
						}
					}
					finding = Finding{
						string(rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm),
						issue}
				}
			}

		}
		results = append(results, finding)
		buckets[i].Encryption = finding.Summary
		buckets[i].EncryptionIssue = finding.Issue
	}

	summarizeIssues(results, "Bucket Encryption")
}

func checkAclSettings(buckets []Bucket, client *s3.Client) {
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		f := getOwnership(client, buckets[i].Name)
		results = append(results, f)
		buckets[i].Ownership = f.Summary
		buckets[i].OwnershipIssue = f.Issue
	}

	summarizeIssues(results, "Bucket ACLs")

	/*
		// Count the number of buckets whose Ownership is not BucketOwnerEnforced
		suboptimalOwnershipBuckets := 0
		for i := range buckets {
			if buckets[i].Ownership != "BucketOwnerEnforced" {
				suboptimalOwnershipBuckets++
			}
		}
		if suboptimalOwnershipBuckets > 0 {
			fmt.Printf("Bucket ACLs: %d buckets have suboptimal settings\n", suboptimalOwnershipBuckets)
		} else {
			fmt.Printf("Bucket ACLs: all good\n")
		}*/
}

func summarizeIssues(results []Finding, checkName string) {

	issueCount := 0
	for _, r := range results {
		if r.Issue != "" {
			issueCount++
		}
	}
	if issueCount > 0 {
		fmt.Printf("%s: recommend changing %d buckets\n", checkName, issueCount)

		// compute the count of each issue
		issueCounts := make(map[string]int)
		for _, r := range results {
			if r.Issue != "" {
				issueCounts[r.Issue]++
			}
		}
		// convert issueCount to slice of IssueWithCount instances
		issueWithCounts := make([]IssueWithCount, 0, len(issueCounts))
		for k, v := range issueCounts {
			issueWithCounts = append(issueWithCounts, IssueWithCount{Issue: k, Count: v})
		}
		// sort issueWithCounts by count
		sort.Slice(issueWithCounts, func(i, j int) bool {
			return issueWithCounts[i].Count > issueWithCounts[j].Count
		})
		// print issuesWithCounts
		for _, i := range issueWithCounts {
			fmt.Printf("- %d buckets: %s\n", i.Count, i.Issue)
		}
	} else {
		fmt.Printf("%s: all good\n", checkName)
	}
	fmt.Printf("\n")
}

func runS3Checks(cfg aws.Config, err error) {
	// Create an S3 service client.
	client := s3.NewFromConfig(cfg)

	// Call the function to list S3 buckets.
	bucketNames, err := listS3Buckets(client)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("Found %d buckets\n\n", len(bucketNames))

	buckets := make([]Bucket, 0, len(bucketNames))
	for _, bucket := range bucketNames {
		// For testing, only focus on interesting buckets
		if strings.HasPrefix(bucket, "joom-analytics-") {
			buckets = append(buckets, Bucket{Name: bucket})
		}
	}
	//buckets = buckets[0:10]

	// Print the bucket names.
	//	fmt.Println("S3 Buckets:")
	//	for _, name := range bucketNames {
	//		fmt.Println(name)
	//	}

	// Create an S3 service client.

	for i := range buckets {
		// Get bucket region
		r, err := client.GetBucketLocation(context.TODO(), &s3.GetBucketLocationInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			buckets[i].Region = err.Error()
		} else {
			buckets[i].Region = string(r.LocationConstraint)
		}
	}

	checkAclSettings(buckets, client)

	checkVersioning(buckets, client)

	checkEncryption(buckets, client)

	/*

		checkInventory(buckets, client)

		for i := range buckets {
			// Get value of bucket's server access logging
			r, err := client.GetBucketLogging(context.TODO(), &s3.GetBucketLoggingInput{
				Bucket: &buckets[i].Name,
			})
			if err != nil {
				buckets[i].Logging = err.Error()
			} else {
				if r.LoggingEnabled == nil {
					buckets[i].Logging = "disabled"
				} else {
					buckets[i].Logging = fmt.Sprintf("%s/%s", *r.LoggingEnabled.TargetBucket, *r.LoggingEnabled.TargetPrefix)
				}
			}
		}


		}*/

	// Convert the array of buckets in 'buckets' to JSON string
	bucketsJson, err := json.Marshal(buckets)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Sending to API")
	err = sendToAPI(bucketsJson)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}

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

type InventoryFile struct {
	Key string
}

type InventoryManifest struct {
	Files []InventoryFile `json:"files"`
}

func uploadInventoryForBucket(cfg aws.Config, bucket string, prefix string) {
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
			fmt.Println("Error:", err)
			return
		}
		objects = append(objects, output.Contents...)
	}
	fmt.Printf("Found %d objects\n", len(objects))

	dates := make([]time.Time, 0, 20)
	dateFormat := "2006-01-02T15-04Z"
	for _, o := range objects {
		key := *o.Key
		// get key substring after prefix
		tail := key[len(prefix)+1:]
		next_slash := strings.Index(tail, "/")
		if next_slash != -1 {
			tail = tail[:next_slash]
			fmt.Printf("Tail: %s\n", tail)
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

	for _, d := range dates {
		fmt.Printf("Date: %s\n", d.Format(time.DateOnly))
	}

	fmt.Printf("Max date: %s\n", maxDate.Format(time.DateTime))
	content, err := readObjectContent(client, bucket, prefix+"/"+maxDate.Format(dateFormat)+"/manifest.json")
	if err != nil {
		fmt.Printf(err.Error())
	} else {
		fmt.Printf("Content: %s\n", content)
	}

	var manifest InventoryManifest
	err = json.Unmarshal(content, &manifest)
	if err != nil {
		fmt.Printf("Could not unmarsal manifest: %s\n", err.Error())
	}
	fmt.Printf("Manifest has %d files\n", len(manifest.Files))

	//for

	//presignClient := s3.NewPresignClient(client)
	//presignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
	//	Bucket: &bucket,
	//	Key:    &prefix + "/" + maxDate.Format(dateFormat) + "/manifest.json",
	//}

	// Filter only

}
func uploadInventory(cfg aws.Config) {
	uploadInventoryForBucket(cfg, "joom-analytics-logs", "s3-inventory/joom-analytics-mart/default")
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	// set region to eu-central-1
	cfg.Region = "eu-central-1"
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	runS3Checks(cfg, err)

	//uploadInventory(cfg)
}
