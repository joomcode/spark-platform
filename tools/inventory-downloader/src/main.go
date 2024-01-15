package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"syscall"
)

func extractFilenameFromURL(urlString string) (string, error) {
	parsedURL, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}

	filename := path.Base(parsedURL.Path)

	return filename, nil
}

func downloadFile(url string) (io.ReadCloser, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %s", response.Status)
	}

	return response.Body, nil
}

func uploadToS3(client *s3.Client, ctx context.Context, reader io.Reader, bucketName, objectKey string) error {
	uploader := manager.NewUploader(client)

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &objectKey,
		Body:   reader,
	})
	return err
}

func processFile(client *s3.Client, ctx context.Context, objectKey string, fileUrl string) error {
	exists, err := keyExists(client, ctx, *bucketName, objectKey)

	if err != nil {
		log.Error().Str("key", objectKey).Str("bucket", *bucketName).Msg("Failed to check file exists")
	}

	if !exists {
		reader, err := downloadFile(fileUrl)
		if err != nil {
			return fmt.Errorf("Error downloading file: %w", err)

		}
		defer reader.Close()

		err = uploadToS3(client, ctx, reader, *bucketName, objectKey)
		if err != nil {
			return fmt.Errorf("Error uploading file to S3: %w", err)
		}
		return nil
	} else {
		log.Info().Str("key", objectKey).Str("bucket", *bucketName).Msg("File allready exists")
		return nil
	}
}

var bucketName = flag.String("bucket", "joom-analytics-landing", "Target bucket to save files")
var targetPrefix = flag.String("prefix", "storage-advisor/inventory", "Prefix to save files")
var brokers = flag.String("brokers", "localhost:29092", "kafka brokers")
var region = flag.String("region", "eu-central-1", "kafka brokers")
var groupId = flag.String("group", "inventory-downloader-gburg", "kafka listener group")
var topic = flag.String("topic", "", "kafka listener group")

type Data struct {
	ProjectId int
	Payload   InventoryManifest
}

func keyExists(client *s3.Client, ctx context.Context, bucket string, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func processPayload(client *s3.Client, ctx context.Context, inputManifest InventoryManifest, projectId int) error {
	var err error
	outputManifest := InventoryManifest{PartitionDate: inputManifest.PartitionDate}

	for _, file := range inputManifest.Files {
		filename, err := extractFilenameFromURL(file.Url)
		if err != nil {
			return fmt.Errorf("Error getting filename form URL: %w", err)
		}

		objectKey := fmt.Sprintf("%s/%s/%s/%s/%s/%s",
			*targetPrefix,
			strconv.Itoa(projectId),
			inputManifest.Bucket,
			inputManifest.PartitionDate,
			"data",
			filename)

		err = processFile(client, ctx, objectKey, file.Url)
		if err != nil {
			return fmt.Errorf("Error processing file: %w", err)
		}
		outputManifest.Files = append(outputManifest.Files, InventoryFile{Key: objectKey, MD5checksum: file.MD5checksum})
	}

	manifestKey := fmt.Sprintf("%s/%s/%s/%s/%s",
		*targetPrefix,
		strconv.Itoa(projectId),
		inputManifest.Bucket,
		inputManifest.PartitionDate,
		"manifest.json")
	manifestBytes, err := json.Marshal(outputManifest)
	if err != nil {
		return fmt.Errorf("Error Marshalling manifest: %w", err)
	}

	err = uploadToS3(client, ctx, bytes.NewReader(manifestBytes), *bucketName, manifestKey)
	if err != nil {
		return fmt.Errorf("Error uploading file to S3: %w", err)
	}

	log.
		Info().
		Str("targetPath", fmt.Sprintf("s3://%s/%s", *bucketName, manifestKey)).
		Msg("Finished processing manifest")

	return nil
}

func doCommit(consumer *kafka.Consumer) {
	info, err := consumer.Commit()
	if err != nil {
		var kafkaerr kafka.Error
		if errors.As(err, &kafkaerr) && kafkaerr.Code() == kafka.ErrNoOffset {
			log.Error().
				Err(err).
				Msg("ErrNoOffset")
		} else {
			log.Error().
				Err(err).
				Msg("Failed to commit")
		}
	} else {
		log.Info().
			Str("topic", *info[len(info)-1].Topic).
			Str("Offset", info[len(info)-1].Offset.String()).
			Msg("Commited Topic")
	}
}

func main() {

	flag.Parse()

	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	cfg.Region = *region

	if err != nil {
		log.Error().
			Err(err).
			Msg("AWS config")
		return
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":        *brokers,
		"group.id":                 *groupId + strconv.Itoa(manifestVersion),
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"go.events.channel.enable": true,
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("Kafka consumer create")
	}

	topics := []string{*topic}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.
			Fatal().
			Err(err).
			Msg("Subscribe to topics")
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	log.Info().
		Strs("topics", topics).
		Msg("Listening to topics %s\n")

	client := s3.NewFromConfig(cfg)

loop:
	for {
		select {
		case sig := <-sigchan:
			log.Warn().
				Str("signal", sig.String()).
				Msg("Caught signal terminating")
			break loop
		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				var data Data
				if err := json.Unmarshal(e.Value, &data); err != nil {
					log.
						Error().
						Err(err).
						Msg("Error decoding JSON")
				} else {
					manifest := data.Payload
					if manifest.Version != manifestVersion {
						log.
							Warn().
							Int("manifestVersion", manifest.Version).
							Str("offset", e.TopicPartition.Offset.String()).
							Int("projectId", data.ProjectId).
							Msg("Skipping manifest")
					} else {
						log.
							Info().
							Str("offset", e.TopicPartition.Offset.String()).
							Int("projectId", data.ProjectId).
							Str("partitionDate", manifest.PartitionDate).
							Msg("Received manifest")
						err = processPayload(client, ctx, manifest, data.ProjectId)
						if err != nil {
							log.
								Error().
								Str("offset", e.TopicPartition.Offset.String()).
								Int("projectId", data.ProjectId).
								Str("partitionDate", manifest.PartitionDate).
								Err(err).
								Msg("Error processing manifest")
						}
						doCommit(consumer)
					}
				}
			case kafka.Error:
				log.Printf("Error: %v\n", e)
				if e.Code() == kafka.ErrAllBrokersDown {
					break loop
				}
			}
		}
	}

	log.
		Warn().
		Msg("Closing consumer")
	if err := consumer.Close(); err != nil {
		log.
			Error().
			Err(err).
			Msg("Error closing consumer")
	}
}
