# syntax=docker/dockerfile:1.7
#
# Two-stage build for the Lattice book. Stage one renders the mdBook
# from `book/` into a static site. Stage two serves the rendered
# bytes behind Caddy.

FROM rust:1.95-alpine AS book-builder
RUN apk add --no-cache build-base
RUN cargo install mdbook --version 0.4.42 --locked --root /usr/local
WORKDIR /work
COPY book ./book
RUN mdbook build book

FROM caddy:2-alpine
COPY --from=book-builder /work/book/book /srv
COPY deploy/Caddyfile /etc/caddy/Caddyfile
EXPOSE 8080
# v1.26 deploy hardening. Probe the served root through Caddy
# every 30 seconds so an orchestrator can spot a wedged
# process and recycle the container. busybox `wget` ships in
# the alpine Caddy image; `--spider` does a HEAD-style
# request without downloading the body.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --spider --quiet "http://127.0.0.1:${PORT:-8080}/" || exit 1
CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
