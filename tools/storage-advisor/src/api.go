package main

import (
	"fmt"
	resty "github.com/go-resty/resty/v2"
)

type Api struct {
	url    string
	token  string
	client *resty.Client
}

func NewApi(url string, token *string) *Api {
	client := resty.New()
	normalized := url
	if normalized[len(normalized)-1] != '/' {
		normalized += "/"
	}
	theToken := ""
	if token != nil {
		theToken = *token
	}
	return &Api{url: normalized, token: theToken, client: client}
}

func (api *Api) IsConfigured() bool {
	return api.token != ""
}

func (api *Api) Post(endpoint string, payload []byte) error {
	resp, err := api.client.R().
		SetAuthToken(api.token).SetBody(payload).Post(api.url + endpoint)

	if err != nil {
		fmt.Printf("Error: could not post to %s: %v\n", endpoint, err.Error())
		return err
	}

	if resp.StatusCode() < 200 || resp.StatusCode() > 299 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	return nil
}
