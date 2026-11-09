# One node of Ruumble Foundation: docker build -t ruumble-foundation .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /foundation ./cmd/foundation

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /foundation /foundation
EXPOSE 64738/tcp
ENTRYPOINT ["/foundation"]
