FROM node:22-bookworm-slim AS frontend
WORKDIR /src/web
COPY web/package.json ./
RUN npm install
COPY web/ ./
RUN npm run build

FROM golang:1.25-bookworm AS backend
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends patch && rm -rf /var/lib/apt/lists/*
# Keep the dependency and upstream-patch layer independent from application
# source. A UI or handler edit must not force a full Go module download again.
COPY go.mod go.sum go.work ./
COPY scripts/prepare-upstream-progress.sh ./scripts/prepare-upstream-progress.sh
COPY patches/tdl-progress-0.20.4.patch ./patches/tdl-progress-0.20.4.patch
COPY patches/tdl-runtime-options-0.20.4.patch ./patches/tdl-runtime-options-0.20.4.patch
RUN sh ./scripts/prepare-upstream-progress.sh && go mod download
COPY . ./
RUN go test ./...
COPY --from=frontend /src/web/dist ./internal/httpapi/static
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tdl ./cmd/tdl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=backend /out/tdl /tdl
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/tdl"]
