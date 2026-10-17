FROM golang:1.25.5-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/gfs ./cmd/gfs

FROM alpine:3.22
RUN addgroup -g 10001 gfs && adduser -D -H -u 10001 -G gfs gfs
COPY --from=build /out/gfs /usr/local/bin/gfs
USER 10001:10001
WORKDIR /work
ENTRYPOINT ["gfs"]
