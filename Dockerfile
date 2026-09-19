FROM node:22-bookworm-slim AS frontend
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
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
COPY patches/tdl-cancel-0.20.4.patch ./patches/tdl-cancel-0.20.4.patch
RUN sh ./scripts/prepare-upstream-progress.sh && go mod download
# Keep the Go test layer independent from Web source changes. The static Web
# bundle is embedded only for the final binary build below.
COPY cmd ./cmd
COPY internal ./internal
ARG RUN_TESTS=1
RUN if [ "$RUN_TESTS" = "1" ]; then go test ./...; fi
COPY --from=frontend /src/web/dist ./internal/httpapi/static
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tdl ./cmd/tdl

FROM gcr.io/distroless/static-debian12
COPY --from=backend /out/tdl /tdl
EXPOSE 8080
ENTRYPOINT ["/tdl"]
