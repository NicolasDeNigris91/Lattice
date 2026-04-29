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
CMD ["caddy", "run", "--config", "/etc/caddy/Caddyfile", "--adapter", "caddyfile"]
