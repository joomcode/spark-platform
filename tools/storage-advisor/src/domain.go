package main

var manifestVersion = 2

type InventoryFile struct {
	Url         string `json:"url,omitempty"`
	Key         string `json:"key,omitempty"`
	MD5checksum string
}

type InventoryManifest struct {
	Files         []InventoryFile `json:"files"`
	Bucket        string          `json:"sourceBucket"`
	PartitionDate string          `json:"partitionDate,omitempty"`
	Version       int             `json:"manifestVersion,omitempty"`
}
