package main

import (
	"fmt"
	resty "github.com/go-resty/resty/v2"
)

type JoomCloudAPI struct {
	url    string
	token  string
	client *resty.Client
}

func NewApi(url string, token *string) *JoomCloudAPI {
	client := resty.New()
	normalized := url
	if normalized[len(normalized)-1] != '/' {
		normalized += "/"
	}
	theToken := ""
	if token != nil {
		theToken = *token
	}
	return &JoomCloudAPI{url: normalized, token: theToken, client: client}
}

func (api *JoomCloudAPI) IsConfigured() bool {
	return api.token != ""
}

func (api *JoomCloudAPI) Post(endpoint string, payload []byte) error {
	fullURL := api.url + endpoint
	resp, err := api.client.R().SetAuthToken(api.token).SetBody(payload).Post(fullURL)

	if err != nil {
		return fmt.Errorf("could not post to %s: %v", fullURL, err)
	}

	if resp.StatusCode() < 200 || resp.StatusCode() > 299 {
		return fmt.Errorf("unexpected status code calling %s: %d", fullURL, resp.StatusCode())
	}

	return nil
}

func (api *JoomCloudAPI) PostS3Buckets(payload []byte) error {
	return api.Post("storage-advisor/buckets", payload)
}
func (api *JoomCloudAPI) PostS3Inventory(payload []byte) error {
	return api.Post("storage-advisor/s3-inventory", payload)
}
