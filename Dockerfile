FROM golang:bullseye as builder
LABEL builder=true multistage_tag="lwodcollector-builder"
WORKDIR /app
COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v

FROM debian:bullseye-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/lwodcollector .
ENTRYPOINT [ "./lwodcollector" ]