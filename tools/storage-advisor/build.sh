#!/bin/bash

set -eo pipefail

VERSION=0.0.12
aws ecr get-login-password | docker login --username AWS --password-stdin 392166590300.dkr.ecr.eu-central-1.amazonaws.com
docker buildx build . -t "392166590300.dkr.ecr.eu-central-1.amazonaws.com/storage-advisor:$VERSION" -f Dockerfile --push
