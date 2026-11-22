FROM golang:1.27.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY sql ./sql
RUN CGO_ENABLED=0 go build -trimpath -o /out/migration-kit .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/migration-kit /migration-kit
USER 65532:65532
ENTRYPOINT ["/migration-kit"]
CMD ["benchmark"]
