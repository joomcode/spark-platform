package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/fatih/color"
	"github.com/manifoldco/promptui"
	"os"
	"sort"
	"strings"
	"text/template"
)

type IssueKind string

const (
	IssueKindVersioningNotSet                 = "VersioningNotSet"
	IssueKindVersioningSuspended              = "VersioningSuspended"
	IssueKindVersioningError                  = "VersioningError"
	IssueKindOwnershipNotSet                  = "OwnershipNotSet"
	IssueKindOwnershipLax                     = "OwnershipLax"
	IssueKindOwnershipError                   = "OwnershipError"
	IssueKindEncryptionKMSWithoutBucketKey    = "EncryptionKMSWithoutBucketKey"
	IssueKindEncryptionAES256WithoutBucketKey = "EncryptionAES256WithoutBucketKey"
	IssueKindEncryptionError                  = "EncryptionError"

	IssueKindInventoryError  = "InventoryError"
	IssueKindInventoryNotSet = "InventoryNotSet"

	IssueKindInventoryNoSuitable = "InventoryNoSuitable"

	IssueKindLoggingError  = "LoggingError"
	IssueKindLoggingNotSet = "LoggingNotSet"
)

type IssueWithCount struct {
	Issue string
	Count int
}

type Finding struct {
	Summary          string
	Issue            string
	IssueDetails     string
	Recommentation   string
	RecommentationTf string
}

func MakeErrorFinding(summary string, issue string, issueDetails string) Finding {
	// Copy issue detail to recomemntations, so that user sees it.
	return Finding{summary, issue, issueDetails, issueDetails, ""}
}

type FindingWithBucket struct {
	Bucket string
	Finding
}

var green = color.New(color.FgGreen).SprintfFunc()
var yellow = color.New(color.FgYellow).Add(color.Bold).SprintfFunc()
var red = color.New(color.FgRed).SprintfFunc()
var bold = color.New(color.Bold).SprintfFunc()

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
	Name                   string `json:"name" bson:"name"`
	Region                 string `json:"region" bson:"region"`
	Ownership              string `json:"ownership" bson:"ownership"`
	OwnershipIssue         string `json:"ownershipIssue" bson:"ownershipIssue"`
	OwnershipIssueDetails  string `json:"ownershipIssueDetails" bson:"ownershipIssueDetails"`
	Versioning             string `json:"versioning" bson:"versioning"`
	VersioningIssue        string `json:"versioningIssue" bson:"versioningIssue"`
	VersioningIssueDetails string `json:"versioningIssueDetails" bson:"versioningIssueDetails"`
	Inventory              string `json:"inventory" bson:"inventory"`
	InventoryIssue         string `json:"inventoryIssue" bson:"inventoryIssue"`
	InventoryIssueDetails  string `json:"inventoryIssueDetails" bson:"inventoryIssueDetails"`
	Logging                string `json:"logging" bson:"logging"`
	LoggingIssue           string `json:"loggingIssue" bson:"loggingIssue"`
	LoggingIssueDetails    string `json:"loggingIssueDetails" bson:"loggingIssueDetails"`
	Encryption             string `json:"encryption" bson:"encryption"`
	EncryptionIssue        string `json:"encryptionIssue" bson:"encryptionIssue"`
	EncryptionIssueDetails string `json:"encryptionIssueDetails" bson:"encryptionIssueDetails"`

	UsableInventory string `json:"usableInventory" bson:"usableInventory"`
}

func getAclSettingsForBucket(client *s3.Client, bucket string) Finding {
	const R = "Set bucket ownership to 'Bucket owner enforced'" // ('Permissions' -> 'Object Ownership' in AWS S3 Console)
	const RT = `
resource "aws_s3_bucket_ownership_controls" "{{ .Bucket }}" {
  bucket = {{ .Bucket }}
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
`
	r, err := client.GetBucketOwnershipControls(context.TODO(), &s3.GetBucketOwnershipControlsInput{Bucket: &bucket})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "OwnershipControlsNotFoundError" {
				return Finding{"Not set", IssueKindOwnershipNotSet, "",
					R, RT}
			} else {
				return MakeErrorFinding("Error", IssueKindOwnershipError, ae.Error())
			}
		} else {
			return MakeErrorFinding("Error", IssueKindOwnershipError, err.Error())
		}
	} else {
		if len(r.OwnershipControls.Rules) == 0 {
			return Finding{"Not set", IssueKindOwnershipNotSet, "",
				R, RT}
		}
		if len(r.OwnershipControls.Rules) > 1 {
			return MakeErrorFinding("Multiple", IssueKindOwnershipError, "Unexpected multiple ownership rules")
		}
		ownership := r.OwnershipControls.Rules[0].ObjectOwnership
		if ownership != types.ObjectOwnershipBucketOwnerEnforced {
			return Finding{string(ownership), IssueKindOwnershipLax, "",
				R, RT}
		}
		return Finding{string(ownership), "", "", "", ""}
	}
}

func checkAclSettings(buckets []Bucket, client *s3.Client, issues *[]FindingWithBucket) {
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		f := getAclSettingsForBucket(client, buckets[i].Name)
		results = append(results, f)
		buckets[i].Ownership = f.Summary
		buckets[i].OwnershipIssue = f.Issue
		buckets[i].OwnershipIssueDetails = f.IssueDetails
		if f.Issue != "" {
			*issues = append(*issues, FindingWithBucket{buckets[i].Name, f})
		}
	}

	summarizeIssues(results, "Bucket ACLs")
}

func checkInventory(buckets []Bucket, client *s3.Client, issues *[]FindingWithBucket) {
	R := "Enable object inventory"
	RT := `
resource "aws_s3_bucket_inventory" "{{ .Bucket }}" {
  bucket = {{ .Bucket }}
  name   = "ParquetDaily"

  included_object_versions = "All"
  optional_fields = ["Size", "LastModifiedDate", "StorageClass", "IntelligentTieringAccessTier"]

  schedule {
    frequency = "Daily"
  }

  destination {
    bucket {
      format     = "Parquet"
      bucket_arn = "arn:aws:s3:::{{ .LogBucket }}"
      // Note that for inventory, S3 will add bucket name to the prefix itself.
      prefix     = "{{ .LogPrefix }}/inventory/"
    }
  }
}
`
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		var finding Finding

		r, err := client.ListBucketInventoryConfigurations(context.TODO(), &s3.ListBucketInventoryConfigurationsInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) {
				if ae.ErrorCode() == "InventoryConfigurationNotFoundError" {
					finding = Finding{"Not set", IssueKindInventoryNotSet, "",
						R, RT}
				} else {
					finding = MakeErrorFinding("Error", IssueKindInventoryError, ae.Error())
				}
			} else {
				finding = MakeErrorFinding("Error", IssueKindInventoryError, err.Error())
			}
		} else {
			issuesPerConfiguration := make(map[string][]string, 0)
			for _, c := range r.InventoryConfigurationList {
				location, issues := checkInventoryConfiguration(c)
				if location != "" {
					if location[len(location)-1] == '/' {
						location = location[0 : len(location)-1]
					}
					location = location + "/" + buckets[i].Name + "/" + *c.Id
					fmt.Printf("%s: usable inventory at %s\n", buckets[i].Name, location)
					buckets[i].UsableInventory = location
					break
				}
				issuesPerConfiguration[*c.Id] = issues
			}
			if buckets[i].UsableInventory == "" {
				details := ""
				for id, issues := range issuesPerConfiguration {
					details = details + fmt.Sprintf("%s: %s ", id, strings.Join(issues, ", "))
				}

				finding = Finding{"No suitable inventory", IssueKindInventoryNoSuitable, details,
					R, RT}
			}
		}
		results = append(results, finding)
		buckets[i].Inventory = finding.Summary
		buckets[i].InventoryIssue = finding.Issue
		buckets[i].InventoryIssueDetails = finding.IssueDetails
		if finding.Issue != "" {
			*issues = append(*issues, FindingWithBucket{buckets[i].Name, finding})
		}
	}

	summarizeIssues(results, "Bucket Inventory")
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

	bucket := *c.Destination.S3BucketDestination.Bucket
	i := strings.LastIndex(bucket, ":")
	bucket = bucket[i+1:]

	return bucket + "/" + *c.Destination.S3BucketDestination.Prefix, []string{}
}

func checkVersioning(buckets []Bucket, client *s3.Client, issues *[]FindingWithBucket) {
	const R = "Enable versioning"
	const RT = `
resource "aws_s3_bucket_versioning" "{{ .Bucket }}" {
  bucket = {{ .Bucket }}
  versioning_configuration {
    status = "Enabled"
  }
}
`
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		var finding Finding
		r, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = MakeErrorFinding("Error", IssueKindVersioningError, err.Error())
		} else {
			status := string(r.Status)
			if status == "" {
				status = "Not set"
			}
			issue := ""
			if r.Status == types.BucketVersioningStatusEnabled {
				// All good
			} else if r.Status == types.BucketVersioningStatusSuspended {
				issue = IssueKindVersioningSuspended
			} else {
				issue = IssueKindVersioningNotSet
			}
			finding = Finding{status, issue, "",
				R, RT}
		}
		results = append(results, finding)
		buckets[i].Versioning = finding.Summary
		buckets[i].VersioningIssue = finding.Issue
		buckets[i].VersioningIssueDetails = finding.IssueDetails
		if finding.Issue != "" {
			*issues = append(*issues, FindingWithBucket{buckets[i].Name, finding})
		}
	}

	summarizeIssues(results, "Bucket Versioning")
}

func checkEncryption(buckets []Bucket, client *s3.Client, issues *[]FindingWithBucket) {
	R := "Enable Bucket Key" // (see 'Properties' -> 'Default Encryption' in the AWS S3 Console)
	RT := `
resource "aws_s3_bucket_server_side_encryption_configuration" "{{ .Bucket }}" {
  bucket = {{ .Bucket }}

  rule {
    bucket_key_enabled = true
  }
}
`
	RTKMS := `
// WARNING: cannot generate safe proposed terraform change.
// This bucket already configures KMS encryption and adding additional
// configuration here can conflict with that configuration. Please find
// the original location where you configure KMS encryption and add
// enable bucket key.
`
	results := make([]Finding, 0, len(buckets))

	for i := range buckets {
		var finding Finding
		// Get bucket encryption
		r, err := client.GetBucketEncryption(context.TODO(), &s3.GetBucketEncryptionInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = MakeErrorFinding("Error", IssueKindEncryptionError, err.Error())
		} else {
			if len(r.ServerSideEncryptionConfiguration.Rules) == 0 {
				// This should not ever happen, really, as S3 applies default encryption in
				// all cases.
				finding = MakeErrorFinding("Error", IssueKindEncryptionError,
					"We've found a bucket with no encryption at all. This is not supposed to happen, ever")
			} else if len(r.ServerSideEncryptionConfiguration.Rules) > 1 {
				finding = MakeErrorFinding("Error", IssueKindEncryptionError,
					"We've found a bucket with multiple encryption rules. This is not supposed to happen, ever")
			} else {
				rule := r.ServerSideEncryptionConfiguration.Rules[0]
				if rule.ApplyServerSideEncryptionByDefault == nil {
					finding = MakeErrorFinding("Error", IssueKindEncryptionError,
						"We've found a bucket with no encryption rules. This is not supposed to happen ever")
				} else {
					issue := ""
					rt := ""
					if rule.BucketKeyEnabled == nil || !*rule.BucketKeyEnabled {
						if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm == types.ServerSideEncryptionAes256 {
							issue = IssueKindEncryptionAES256WithoutBucketKey
							rt = RT
						} else {
							issue = IssueKindEncryptionKMSWithoutBucketKey
							rt = RTKMS
						}
					}
					finding = Finding{
						string(rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm),
						issue, "", R, rt}
				}
			}
		}
		results = append(results, finding)
		buckets[i].Encryption = finding.Summary
		buckets[i].EncryptionIssue = finding.Issue
		buckets[i].EncryptionIssueDetails = finding.IssueDetails
		if finding.Issue != "" {
			*issues = append(*issues, FindingWithBucket{buckets[i].Name, finding})
		}
	}

	summarizeIssues(results, "Bucket Encryption")
}

func checkLogging(buckets []Bucket, client *s3.Client, issues *[]FindingWithBucket) {
	R := "Enable S3 logging" // (see 'Properties' -> 'Server access logging' in the AWS S3 Console)
	RT := `
resource "aws_s3_bucket_logging" "{{ .Bucket }}" {
  bucket = {{ .Bucket }}

  target_bucket = "{{ .LogBucket }}"
  // We need to explicitly add bucket name in the prefix.
  target_prefix = "{{ .LogPrefix }}/logs/{{ .Bucket }}/"
}
`
	results := make([]Finding, 0, len(buckets))

	for i := range buckets {
		var finding Finding
		// Get bucket logging
		r, err := client.GetBucketLogging(context.TODO(), &s3.GetBucketLoggingInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = MakeErrorFinding("Error", IssueKindLoggingError, err.Error())
		} else {
			if r.LoggingEnabled == nil {
				finding = Finding{"Not enabled", IssueKindLoggingNotSet, "",
					R, RT}
			} else {
				path := fmt.Sprintf("%s/%s", *r.LoggingEnabled.TargetBucket, *r.LoggingEnabled.TargetPrefix)
				finding = Finding{path, "", "", "", ""}
			}
		}
		results = append(results, finding)
		buckets[i].Logging = finding.Summary
		buckets[i].LoggingIssue = finding.Issue
		buckets[i].LoggingIssueDetails = finding.IssueDetails
		if finding.Issue != "" {
			*issues = append(*issues, FindingWithBucket{buckets[i].Name, finding})
		}
	}

	summarizeIssues(results, "Bucket Logging")
}

func summarizeIssues(results []Finding, checkName string) {

	issueCount := 0
	for _, r := range results {
		if r.Issue != "" {
			issueCount++
		}
	}
	if issueCount > 0 {
		ok := len(results) - issueCount
		fmt.Printf("%s: %s, %s\n", bold(checkName), green("%d OK", ok), yellow("%d warning", issueCount))

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
		fmt.Printf("%s: %s\n", bold(checkName), green("%d OK", len(results)))
	}
	fmt.Printf("\n")
}

func runS3Checks(cfg aws.Config, api *Api, identity string) []Bucket {
	fmt.Printf(green("Running S3 checks\n\n"))

	client := s3.NewFromConfig(cfg)

	bucketNames, err := listS3Buckets(client)
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			color.Red("Could not list buckets: %s: %s", ae.ErrorCode(), ae.ErrorMessage())
		} else {
			color.Red("Could not list buckets: %s", err.Error())
		}
		return []Bucket{}
	}

	fmt.Printf("Found %d buckets\n\n", len(bucketNames))

	buckets := make([]Bucket, 0, len(bucketNames))
	for _, bucket := range bucketNames {
		// For testing, only focus on interesting buckets
		if strings.HasPrefix(bucket, "joom-analytics-") {
			buckets = append(buckets, Bucket{Name: bucket})
		}
	}
	buckets = buckets[0:10]

	for i := range buckets {
		// Get bucket region
		r, err := client.GetBucketLocation(context.TODO(), &s3.GetBucketLocationInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			buckets[i].Region = "Error" //err.Error()
		} else {
			buckets[i].Region = string(r.LocationConstraint)
		}
	}

	issues := make([]FindingWithBucket, 0)

	checkAclSettings(buckets, client, &issues)

	checkVersioning(buckets, client, &issues)

	checkEncryption(buckets, client, &issues)

	checkLogging(buckets, client, &issues)

	checkInventory(buckets, client, &issues)

	writeBasicIssues(issues, identity)

	if api.IsConfigured() {
		uploadBasicIssues(api, buckets)
	}

	return buckets
}

func writeBasicIssues(issues []FindingWithBucket, identity string) {
	// Group issues by buckets
	issuesByBucket := make(map[string][]FindingWithBucket)
	for _, issue := range issues {
		issuesByBucket[issue.Bucket] = append(issuesByBucket[issue.Bucket], issue)
	}

	f, err := os.Create("s3-recommendations.txt")
	if err != nil {
		fmt.Printf("Cannot write issues list: cannot open 's3-recommendations.txt': %s\n", err.Error())
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "\nS3 issue check was run as %s\n", identity)

	if len(issues) == 0 {
		fmt.Fprint(f, "\nThere are not issues. You rock!\n")
		return
	}

	fmt.Fprint(f, "\nPlease consider making the following changes:\n\n")

	for bucket, issues := range issuesByBucket {
		fmt.Fprintf(f, "%s:\n", bucket)
		for _, issue := range issues {
			fmt.Fprintf(f, "  - %s\n", issue.Recommentation)
		}
		fmt.Fprintf(f, "\n")
	}
	fmt.Println(green("Wrote recommentations to 's3-recommentations.txt'\n"))

	fmt.Println("We can write recommendations in Terraform format, which might be easier to apply.")
	fmt.Println("Note that we just write Terraform code, we'll never try to change anything in your AWS account.")

	prompt := promptui.Select{
		Label: "Proceed>",
		Items: []string{"Yes, write Terraform recommendations", "No, skip Terraform recommendations"},
	}

	index, result, err := prompt.Run()
	if err != nil {
		fmt.Printf("Could not get the prompt: %s\n", err.Error())
		return
	}
	if index == 1 {
		return
	}

	fmt.Println("We'll need to know which bucket is good for S3 logs and inventory.")

	validate := func(input string) error {
		if input == "" {
			return errors.New("Bucket name cannot be empty")
		}
		si := strings.Index(input, "/")
		if si == -1 {
			return errors.New("Please specify both bucket and prefix, separated by '/'")
		}
		if si == len(input)-1 {
			return errors.New("Prefix is empty")
		}
		if si < 3 {
			return errors.New("Bucket name is too short")
		}
		return nil
	}
	prompt2 := promptui.Prompt{
		Label:    "Specify the bucket/prefix for logging and inventory",
		Validate: validate,
	}
	result, _ = prompt2.Run()
	si := strings.Index(result, "/")
	logBucket := result[0:si]
	logPrefix := result[si+1:]
	if logPrefix[len(logPrefix)-1] == '/' {
		logPrefix = logPrefix[0 : len(logPrefix)-1]
	}

	tf, err := os.Create("s3-recommendations.tf")
	if err != nil {
		fmt.Printf("Cannot write issues list: cannot open 's3-recommentations.tf': %s\n", err.Error())
		return
	}

	header := `
// These are recommended changes to you S3 setup.
// Note that depending on how you manager your infrastructure, you might already have
// conflicting changes in Terraform, or other configuration system. Please carefully
// review the plan before applying.
// These recommentations were created by Joom Storage Advisor
// It was run as %s
`

	fmt.Fprintf(tf, header, identity)
	defer tf.Close()

	for bucket, issues := range issuesByBucket {

		context := map[string]interface{}{
			"Bucket":    bucket,
			"LogBucket": logBucket,
			"LogPrefix": logPrefix,
		}

		for _, issue := range issues {
			issueHeader := `
// Bucket: %s
// Issue: %s %s
// Recommentation: %s
`
			fmt.Fprintf(tf, issueHeader, bucket, issue.Issue, issue.IssueDetails, issue.Recommentation)
			if issue.RecommentationTf != "" {
				tmpl, err := template.New("tf").Parse(issue.RecommentationTf)
				if err != nil {
					fmt.Fprintf(tf, "// Internal error: %s\n", err.Error())
				}
				err = tmpl.Execute(tf, context)
				if err != nil {
					fmt.Fprintf(tf, "// Internal error: %s\n", err.Error())
				}
			} else {
				fmt.Fprintf(tf, "// Sadly, we coulnd not generate Terraform recommendation\n\n")
			}
		}
	}
	fmt.Println(green("Wrote Terraform recommedations to 's3-recommentations.tf'\n"))
}

func uploadBasicIssues(api *Api, buckets []Bucket) {
	bucketsJson, err := json.Marshal(buckets)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Sending to API")
	err = api.Post("storage-advisor/buckets", bucketsJson)
	if err != nil {
		fmt.Println("Unable to send buckets list to the API:", err.Error())
		return
	}
}
