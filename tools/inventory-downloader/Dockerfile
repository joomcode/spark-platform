FROM golang:1.20.12 AS build

WORKDIR /src

COPY src/go.mod ./
COPY src/go.sum ./

RUN go mod download

COPY src ./

RUN go build -o /inventory-downloader .

FROM ubuntu AS bin

RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates

COPY --from=build /inventory-downloader /inventory-downloader
ENTRYPOINT ["/inventory-downloader"]

LABEL com.joom.retention.maxCount=5
LABEL com.joom.retention.maxCountGroup=develop
LABEL com.joom.retention.pullProtectDays=180
