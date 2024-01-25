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
ENTRYPOINT ["/storage-advisor"]

ENV USER_NAME=advisor
RUN addgroup --gid 1001 $USER_NAME && \
    adduser --ingroup $USER_NAME --shell /bin/false --disabled-password --uid 1001 $USER_NAME
USER $USER_NAME

LABEL com.joom.retention.maxCount=5
LABEL com.joom.retention.maxCountGroup=develop
LABEL com.joom.retention.pullProtectDays=180
