FROM node:22-bookworm-slim AS frontend
WORKDIR /src/web
COPY web/package.json ./
RUN npm install
COPY web/ ./
RUN npm run build

FROM golang:1.25-bookworm AS backend
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends patch && rm -rf /var/lib/apt/lists/*
COPY go.mod ./
COPY . ./
RUN sh ./scripts/prepare-upstream-progress.sh && go mod download
COPY --from=frontend /src/web/dist ./internal/httpapi/static
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tdl ./cmd/tdl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=backend /out/tdl /tdl
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/tdl"]
