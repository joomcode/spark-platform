FROM golang:1.20.12 AS build

WORKDIR /src

COPY src/go.mod ./
COPY src/go.sum ./

RUN go mod download

COPY src ./

RUN go build -o /storage-advisor .

FROM ubuntu AS bin

RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates

COPY --from=build /storage-advisor /storage-advisor
ENTRYPOINT ["/storage-advisor", "--cli_mode=false"]

LABEL com.joom.retention.maxCount=5
LABEL com.joom.retention.maxCountGroup=develop
LABEL com.joom.retention.pullProtectDays=180
