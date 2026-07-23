FROM golang:1.25.4-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server

FROM alpine:3.22
COPY --from=build /out/server /usr/local/bin/server
EXPOSE 6379
# HEALTHCHECK returns in T1.4: probing the port would kill the current
# single-connection prototype, which exits when its one client disconnects.
ENTRYPOINT ["server"]
