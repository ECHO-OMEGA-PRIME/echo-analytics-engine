# Echo Analytics Engine

> Metrics-collection Cloudflare Worker for ECHO Prime (v1.0.0). Gathers metrics
> from across the ECHO service mesh, stores point-in-time snapshots and daily
> rollups in D1, and serves dashboard / growth / trend views.

Private to Echo Prime Technologies.

## What it does

On demand (`POST /snapshot`) it pulls metrics from the bound ECHO services, writes
a row to `metrics_snapshots`, maintains `daily_rollups` and `growth_tracking`, and
exposes read endpoints for dashboards and growth/trend analysis.

## API

Auth: `X-Echo-API-Key`. CORS preflight (`OPTIONS`) supported.

| Method | Route | Purpose |
|---|---|---|
| `GET`  | `/` | Service info |
| `GET`  | `/health` | Liveness |
| `GET`  | `/dashboard` | Current dashboard metrics |
| `GET`  | `/growth` | Growth tracking |
| `GET`  | `/metrics/<service>` | Metrics for a specific service |
| `GET`  | `/trends` | Trend analysis over time |
| `POST` | `/snapshot` | Capture a metrics snapshot now |

## Storage (D1 — `schema.sql`)

| Table | Holds |
|---|---|
| `metrics_snapshots` | Point-in-time metric captures |
| `daily_rollups` | Per-day aggregates |
| `growth_tracking` | Growth series |

## Bindings

`DB` (D1), `CACHE` (KV), `ECHO_API_KEY`, plus service bindings it samples:
`ENGINE_RUNTIME`, `KNOWLEDGE_FORGE`, `SHARED_BRAIN`, `DOCTRINE_FORGE`,
`AUTONOMOUS_DAEMON` (declared in `wrangler.toml`).

## Develop

```bash
npm install
npm run dev          # wrangler dev
npm run cf-typegen   # regenerate Worker types from bindings
npm run deploy       # wrangler deploy
```

Apply `schema.sql` to the D1 database before first run. Secrets live in
`wrangler.toml` / the Cloudflare dashboard — never commit them.

## License

Proprietary — © Echo Prime Technologies. All rights reserved.
