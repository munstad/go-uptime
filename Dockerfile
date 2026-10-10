FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /uptime .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /uptime /usr/local/bin/uptime
EXPOSE 8080
ENTRYPOINT ["uptime"]
CMD ["https://go.dev", "https://github.com"]
