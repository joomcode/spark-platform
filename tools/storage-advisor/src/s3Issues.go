package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Recommendation   string
	RecommendationTF string
}

func makeErrorFinding(summary string, issue string, issueDetails string) Finding {
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

func getACLSettingsForBucket(ctx context.Context, client *s3.Client, bucket string) Finding {
	const R = "Set bucket ownership to 'Bucket owner enforced'" // ('Permissions' -> 'Object Ownership' in AWS S3 Console)
	const RT = `
resource "aws_s3_bucket_ownership_controls" "{{ .BucketResourceName }}" {
  bucket = "{{ .Bucket }}"
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
`
	r, err := client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: &bucket})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "OwnershipControlsNotFoundError" {
				return Finding{"Not set", IssueKindOwnershipNotSet, "",
					R, RT}
			} else {
				return makeErrorFinding("Error", IssueKindOwnershipError, ae.Error())
			}
		} else {
			return makeErrorFinding("Error", IssueKindOwnershipError, err.Error())
		}
	} else {
		if len(r.OwnershipControls.Rules) == 0 {
			return Finding{"Not set", IssueKindOwnershipNotSet, "",
				R, RT}
		}
		if len(r.OwnershipControls.Rules) > 1 {
			return makeErrorFinding("Multiple", IssueKindOwnershipError, "Unexpected multiple ownership rules")
		}
		ownership := r.OwnershipControls.Rules[0].ObjectOwnership
		if ownership != types.ObjectOwnershipBucketOwnerEnforced {
			return Finding{string(ownership), IssueKindOwnershipLax, "",
				R, RT}
		}
		return Finding{string(ownership), "", "", "", ""}
	}
}

func checkACLSettings(ctx context.Context, client *s3.Client, buckets []Bucket, issues *[]FindingWithBucket) {
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		f := getACLSettingsForBucket(ctx, client, buckets[i].Name)
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

func checkInventory(ctx context.Context, client *s3.Client, buckets []Bucket, issues *[]FindingWithBucket) {
	R := "Enable object inventory"
	RT := `
resource "aws_s3_bucket_inventory" "{{ .BucketResourceName }}" {
  bucket = "{{ .Bucket }}"
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

		r, err := client.ListBucketInventoryConfigurations(ctx, &s3.ListBucketInventoryConfigurationsInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) {
				if ae.ErrorCode() == "InventoryConfigurationNotFoundError" {
					finding = Finding{"Not set", IssueKindInventoryNotSet, "",
						R, RT}
				} else {
					finding = makeErrorFinding("Error", IssueKindInventoryError, ae.Error())
				}
			} else {
				finding = makeErrorFinding("Error", IssueKindInventoryError, err.Error())
			}
		} else {
			issuesPerConfiguration := map[string][]string{}
			for _, c := range r.InventoryConfigurationList {
				location, issues := checkInventoryConfiguration(c)
				if location != "" {
					if location[len(location)-1] == '/' {
						location = location[0 : len(location)-1]
					}
					location = location + "/" + buckets[i].Name + "/" + *c.Id
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

func checkVersioning(ctx context.Context, client *s3.Client, buckets []Bucket, issues *[]FindingWithBucket) {
	const R = "Enable versioning"
	const RT = `
resource "aws_s3_bucket_versioning" "{{ .BucketResourceName }}" {
  bucket = "{{ .Bucket }}"
  versioning_configuration {
    status = "Enabled"
  }
}
`
	results := make([]Finding, 0, len(buckets))
	for i := range buckets {
		var finding Finding
		r, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = makeErrorFinding("Error", IssueKindVersioningError, err.Error())
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

func checkEncryption(ctx context.Context, client *s3.Client, buckets []Bucket, issues *[]FindingWithBucket) {
	R := "Enable Bucket Key" // (see 'Properties' -> 'Default Encryption' in the AWS S3 Console)
	RT := `
resource "aws_s3_bucket_server_side_encryption_configuration" "{{ .BucketResourceName }}" {
  bucket = "{{ .Bucket }}"

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
		r, err := client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = makeErrorFinding("Error", IssueKindEncryptionError, err.Error())
		} else {
			if len(r.ServerSideEncryptionConfiguration.Rules) == 0 {
				// This should not ever happen, really, as S3 applies default encryption in
				// all cases.
				finding = makeErrorFinding("Error", IssueKindEncryptionError,
					"We've found a bucket with no encryption at all. This is not supposed to happen, ever")
			} else if len(r.ServerSideEncryptionConfiguration.Rules) > 1 {
				finding = makeErrorFinding("Error", IssueKindEncryptionError,
					"We've found a bucket with multiple encryption rules. This is not supposed to happen, ever")
			} else {
				rule := r.ServerSideEncryptionConfiguration.Rules[0]
				if rule.ApplyServerSideEncryptionByDefault == nil {
					finding = makeErrorFinding("Error", IssueKindEncryptionError,
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

func checkLogging(ctx context.Context, client *s3.Client, buckets []Bucket, issues *[]FindingWithBucket) {
	R := "Enable S3 logging" // (see 'Properties' -> 'Server access logging' in the AWS S3 Console)
	RT := `
resource "aws_s3_bucket_logging" "{{ .BucketResourceName }}" {
  bucket = "{{ .Bucket }}"

  target_bucket = "{{ .LogBucket }}"
  // We need to explicitly add bucket name in the prefix.
  target_prefix = "{{ .LogPrefix }}/logs/{{ .Bucket }}/"
}
`
	results := make([]Finding, 0, len(buckets))

	for i := range buckets {
		var finding Finding
		// Get bucket logging
		r, err := client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			finding = makeErrorFinding("Error", IssueKindLoggingError, err.Error())
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

// Run S3 checks and return the list of buckets with findings and issues, as well
// as separate global list of all issues.
// The function tries to run and return something in face of all errors, and will only
// return error is we can't do anything -- like we can't list buckets.
func runS3Checks(ctx context.Context, s3client *s3.Client) ([]Bucket, []FindingWithBucket, error) {
	fmt.Printf(green("Running S3 checks\n\n"))

	buckets, err := listBuckets(s3client)
	if err != nil {
		return nil, nil, err
	}

	for i := range buckets {
		// Get bucket region
		r, err := s3client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: &buckets[i].Name,
		})
		if err != nil {
			buckets[i].Region = "Error" //err.Error()
		} else {
			buckets[i].Region = string(r.LocationConstraint)
		}
	}

	issues := make([]FindingWithBucket, 0)

	checkACLSettings(ctx, s3client, buckets, &issues)

	checkVersioning(ctx, s3client, buckets, &issues)

	checkEncryption(ctx, s3client, buckets, &issues)

	checkLogging(ctx, s3client, buckets, &issues)

	checkInventory(ctx, s3client, buckets, &issues)

	return buckets, issues, nil
}

func listBuckets(s3client *s3.Client) ([]Bucket, error) {
	bucketNames, err := listS3Buckets(s3client)
	if err != nil {
		return nil, err
	}

	buckets := make([]Bucket, 0, len(bucketNames))
	for _, bucket := range bucketNames {
		buckets = append(buckets, Bucket{Name: bucket})
	}
	return buckets, nil
}

// Write the detailed list of found issues to a file.
// Optionally, write a Terraform file with recommendations.
// This function is called only when we run in CLI mode.
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
			fmt.Fprintf(f, "  - %s\n", issue.Recommendation)
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

	fmt.Println("We need to know which bucket you want to use for S3 logs and inventory, so we can specify that in the generated TF script..")

	validate := func(input string) error {
		if input == "" {
			return errors.New("Bucket name cannot be empty")
		}
		input = strings.TrimPrefix(input, "s3://")
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
		Label:    "Specify the bucket/prefix for logging and inventory, e.g. 'acme-logs/s3'",
		Validate: validate,
	}
	result, _ = prompt2.Run()
	// validator above makes sure there is a slash
	result = strings.TrimPrefix(result, "s3://")
	logBucket, logPrefix, _ := strings.Cut(result, "/")
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

		// Terraform resource names are stricker than bucket names.
		bucketResourceName := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
				(r >= '0' && r <= '9') || r == '_' || r == '-' {
				return r
			}
			return '_'
		}, bucket)

		context := map[string]interface{}{
			"Bucket":             bucket,
			"BucketResourceName": bucketResourceName,
			"LogBucket":          logBucket,
			"LogPrefix":          logPrefix,
		}

		for _, issue := range issues {
			issueHeader := `
// Bucket: %s
// Issue: %s %s
// Recommendation: %s
`
			fmt.Fprintf(tf, issueHeader, bucket, issue.Issue, issue.IssueDetails, issue.Recommendation)
			if issue.RecommendationTF != "" {
				tmpl, err := template.New("tf").Parse(issue.RecommendationTF)
				if err != nil {
					fmt.Fprintf(tf, "// Internal error: %s\n", err.Error())
				}
				err = tmpl.Execute(tf, context)
				if err != nil {
					fmt.Fprintf(tf, "// Internal error: %s\n", err.Error())
				}
			} else {
				fmt.Fprintf(tf, "// Sadly, we could not generate Terraform recommendation\n\n")
			}
		}
	}
	fmt.Println(green("Wrote Terraform recommedations to 's3-recommentations.tf'\n"))
}

func uploadBasicIssues(api *JoomCloudAPI, buckets []Bucket) {
	bucketsJson, err := json.Marshal(buckets)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Sending to API")
	err = api.PostS3Buckets(bucketsJson)
	if err != nil {
		fmt.Println("Unable to send buckets list to the API:", err)
		return
	}
}
