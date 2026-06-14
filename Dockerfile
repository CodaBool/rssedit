FROM golang:1.26-alpine AS build

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY main.go .

RUN go mod init rssedit \
  && go mod tidy \
  && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/rssedit .

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/rssedit /rssedit

EXPOSE 9933

ENTRYPOINT ["/rssedit"]
