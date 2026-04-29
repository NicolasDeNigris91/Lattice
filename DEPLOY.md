# Deploying Lattice

This document covers the two production-side moving parts:

1. The book at `lattice.nicolaspilegidenigris.dev` (Railway)
2. The crate publication on `crates.io` (GitHub Actions)

Neither is required to use the project locally. Both are provided so
that a fresh fork can reproduce the published artifacts.

## Book on Railway

The book renders from `book/` via `mdbook` and is served behind Caddy.

Files involved:

- `deploy/Dockerfile` — two-stage build, mdBook then Caddy.
- `deploy/Caddyfile` — single virtual host bound to `$PORT`, gzip/zstd
  encoding, sane security headers, long cache for static assets, short
  cache for HTML.
- `railway.json` — points Railway at `deploy/Dockerfile`, configures
  health check and restart policy.

To deploy on Railway:

1. Create a new service from this repo on https://railway.com.
2. Railway picks up `railway.json`, builds the Dockerfile, and exposes
   the listening port automatically.
3. Add a custom domain `lattice.nicolaspilegidenigris.dev` in Railway's
   settings and point a CNAME at the assigned hostname.
4. Pushes to `main` redeploy.

To preview locally:

```bash
docker build -f deploy/Dockerfile -t lattice-book .
docker run --rm -p 8080:8080 lattice-book
# open http://localhost:8080
```

To rebuild only the book without Docker:

```bash
cargo install mdbook --version 0.4.42 --locked
mdbook serve book      # live preview at http://localhost:3000
mdbook build book      # static output at book/book/
```

## Crate publication on crates.io

The release workflow at `.github/workflows/release.yml` triggers on
tags matching `v[1-9]+.*.*`. It publishes `lattice-core` and
`lattice-cli` to crates.io and creates a GitHub release.

Pre-1.0 tags (v0.x) only run the GitHub release job; they do not
publish.

To enable the publish step:

1. Generate an API token at https://crates.io/me. Scope it to
   "publish-update" only.
2. Add it as a repository secret named `CARGO_REGISTRY_TOKEN` at
   `https://github.com/NicolasDeNigris91/Lattice/settings/secrets/actions`.
3. The next `v[1-9]+.*.*` tag push will publish.

If the secret is absent the workflow logs a warning and skips publish
without failing the run.

To publish manually:

```bash
cargo login                                   # paste your token
cargo publish -p lattice-core
cargo publish -p lattice-cli                  # after lattice-core is up
```

`lattice-core` must be published before `lattice-cli`, since the CLI
depends on it.
