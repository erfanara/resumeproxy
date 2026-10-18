# Local builds; CI publishes images with ko (see .github/workflows/release.yml).
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /resumeproxy .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /resumeproxy /resumeproxy
ENTRYPOINT ["/resumeproxy"]
