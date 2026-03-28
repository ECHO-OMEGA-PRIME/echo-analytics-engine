/**
 * echo-analytics-engine — v1.0.0
 * Cloudflare Worker: collects metrics from all ECHO services,
 * stores snapshots in D1, serves dashboard/growth/trend endpoints.
 */

// ─── Types ────────────────────────────────────────────────────────────────────

export interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ENGINE_RUNTIME: Fetcher;
  SHARED_BRAIN: Fetcher;
  DOCTRINE_FORGE: Fetcher;
  KNOWLEDGE_FORGE: Fetcher;
  AUTONOMOUS_DAEMON: Fetcher;
  VERSION: string;
  SERVICE_NAME: string;
  ECHO_API_KEY: string;
}

interface MetricSnapshot {
  source: string;
  metric_name: string;
  metric_value: number;
  metadata?: Record<string, unknown>;
}

interface GrowthRow {
  date: string;
  engines_count: number;
  doctrines_count: number;
  knowledge_docs: number;
  brain_messages: number;
  brain_instances: number;
  fleet_score: number;
  bot_posts_total: number;
  queries_total: number;
}

interface CurrentMetrics {
  engines: number;
  doctrines: number;
  queries: number;
  knowledge_docs: number;
  brain_messages: number;
  brain_instances: number;
  fleet_score: number;
  daemon_cycles: number;
}

interface GrowthDelta {
  engines: string;
  doctrines: string;
  queries: string;
  knowledge_docs: string;
  brain_messages: string;
  fleet_score: string;
  daemon_cycles: string;
}

interface DashboardResponse {
  ok: boolean;
  service: string;
  timestamp: string;
  current: CurrentMetrics;
  growth_24h: GrowthDelta;
  growth_7d: GrowthDelta;
  trends: GrowthRow[];
}

interface SourceMetric {
  id: number;
  timestamp: string;
  source: string;
  metric_name: string;
  metric_value: number;
  metadata: string;
}

// ─── Logging ─────────────────────────────────────────────────────────────────

function log(level: 'info' | 'warn' | 'error', message: string, data?: Record<string, unknown>): void {
  const entry = {
    level,
    ts: new Date().toISOString(),
    service: 'echo-analytics-engine',
    message,
    ...data,
  };
  if (level === 'error') {
    console.error(JSON.stringify(entry));
  } else {
    console.log(JSON.stringify(entry));
  }
}

// ─── CORS ─────────────────────────────────────────────────────────────────────

const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Echo-API-Key, Authorization',
  'Access-Control-Max-Age': '86400',
};

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}

function err(message: string, status = 400): Response {
  return json({ ok: false, error: message, ts: new Date().toISOString() }, status);
}

// ─── Auth ─────────────────────────────────────────────────────────────────────

function isAuthorized(req: Request, env: Env): boolean {
  if (!env.ECHO_API_KEY) return false;
  const apiKey = req.headers.get('X-Echo-API-Key');
  if (apiKey && apiKey === env.ECHO_API_KEY) return true;
  const authHeader = req.headers.get('Authorization') || '';
  if (authHeader.startsWith('Bearer ') && authHeader.slice(7) === env.ECHO_API_KEY) return true;
  return false;
}

// ─── Service Fetchers ─────────────────────────────────────────────────────────

async function fetchServiceHealth(binding: Fetcher, path: string, name: string, apiKey: string): Promise<Record<string, unknown>> {
  try {
    const res = await binding.fetch(`http://internal${path}`, {
      headers: { 'X-Echo-API-Key': apiKey },
    });
    if (!res.ok) {
      log('warn', `${name} health non-200`, { status: res.status, path });
      return {};
    }
    const text = await res.text();
    try {
      return JSON.parse(text) as Record<string, unknown>;
    } catch {
      return { raw: text };
    }
  } catch (e) {
    log('error', `${name} fetch failed`, { path, error: String(e) });
    return {};
  }
}

function safeNum(v: unknown, fallback = 0): number {
  if (typeof v === 'number') return v;
  if (typeof v === 'string') {
    const n = parseFloat(v);
    return isNaN(n) ? fallback : n;
  }
  return fallback;
}

function nestedNum(obj: Record<string, unknown>, ...keys: string[]): number {
  let cur: unknown = obj;
  for (const k of keys) {
    if (cur === null || typeof cur !== 'object') return 0;
    cur = (cur as Record<string, unknown>)[k];
  }
  return safeNum(cur);
}

// ─── Metrics Collection ───────────────────────────────────────────────────────

interface CollectedMetrics {
  engines: number;
  doctrines: number;
  queries: number;
  knowledge_docs: number;
  brain_messages: number;
  brain_instances: number;
  fleet_score: number;
  daemon_cycles: number;
  bot_posts_total: number;
}

async function collectAllMetrics(env: Env): Promise<CollectedMetrics> {
  log('info', 'collecting metrics from all services');

  const [engineData, brainData, doctrineData, knowledgeData, daemonData] = await Promise.all([
    fetchServiceHealth(env.ENGINE_RUNTIME, '/health', 'engine-runtime', env.ECHO_API_KEY),
    fetchServiceHealth(env.SHARED_BRAIN, '/health', 'shared-brain', env.ECHO_API_KEY),
    fetchServiceHealth(env.DOCTRINE_FORGE, '/health', 'doctrine-forge', env.ECHO_API_KEY),
    fetchServiceHealth(env.KNOWLEDGE_FORGE, '/health', 'knowledge-forge', env.ECHO_API_KEY),
    fetchServiceHealth(env.AUTONOMOUS_DAEMON, '/status', 'autonomous-daemon', env.ECHO_API_KEY),
  ]);

  // Engine Runtime: engines_loaded, total_doctrines, total_queries
  const engines = safeNum(
    engineData.engines_loaded ?? engineData.engines ?? nestedNum(engineData, 'stats', 'engines_loaded'),
  );
  const doctrines = safeNum(
    engineData.total_doctrines ?? engineData.doctrines ?? nestedNum(engineData, 'stats', 'total_doctrines'),
  );
  const queries = safeNum(
    engineData.total_queries ?? engineData.queries ?? nestedNum(engineData, 'stats', 'total_queries'),
  );

  // Shared Brain: stats.total_messages, stats.total_instances
  const brain_messages = safeNum(
    nestedNum(brainData, 'stats', 'total_messages') || (brainData.total_messages ?? brainData.messages ?? 0),
  );
  const brain_instances = safeNum(
    nestedNum(brainData, 'stats', 'total_instances') || (brainData.total_instances ?? brainData.instances ?? 0),
  );

  // Knowledge Forge: stats.documents
  const knowledge_docs = safeNum(
    nestedNum(knowledgeData, 'stats', 'documents') || (knowledgeData.documents ?? knowledgeData.docs ?? 0),
  );

  // Autonomous Daemon: state.fleetScore, state.cycles
  const fleet_score = safeNum(
    nestedNum(daemonData, 'state', 'fleetScore') || (daemonData.fleetScore ?? daemonData.fleet_score ?? 0),
  );
  const daemon_cycles = safeNum(
    nestedNum(daemonData, 'state', 'cycles') || (daemonData.cycles ?? daemonData.total_cycles ?? 0),
  );

  // Doctrine Forge: stats.total_doctrines or total_generated
  const bot_posts_total = safeNum(
    nestedNum(doctrineData, 'stats', 'total_generated') || (doctrineData.total_generated ?? doctrineData.bot_posts ?? 0),
  );

  const metrics: CollectedMetrics = {
    engines,
    doctrines,
    queries,
    knowledge_docs,
    brain_messages,
    brain_instances,
    fleet_score,
    daemon_cycles,
    bot_posts_total,
  };

  log('info', 'metrics collected', { ...metrics });
  return metrics;
}

// ─── D1 Helpers ───────────────────────────────────────────────────────────────

async function insertSnapshots(db: D1Database, snapshots: MetricSnapshot[]): Promise<void> {
  const stmt = db.prepare(
    'INSERT INTO metrics_snapshots (source, metric_name, metric_value, metadata) VALUES (?, ?, ?, ?)',
  );
  const batch = snapshots.map((s) =>
    stmt.bind(s.source, s.metric_name, s.metric_value, JSON.stringify(s.metadata ?? {})),
  );
  await db.batch(batch);
}

async function upsertDailyRollup(db: D1Database, date: string, source: string, metricName: string): Promise<void> {
  await db
    .prepare(
      `INSERT INTO daily_rollups (date, source, metric_name, min_val, max_val, avg_val, count)
       SELECT ?, ?, ?, MIN(metric_value), MAX(metric_value), AVG(metric_value), COUNT(*)
       FROM metrics_snapshots
       WHERE date(timestamp) = ? AND source = ? AND metric_name = ?
       ON CONFLICT(date, source, metric_name) DO UPDATE SET
         min_val = excluded.min_val,
         max_val = excluded.max_val,
         avg_val = excluded.avg_val,
         count   = excluded.count`,
    )
    .bind(date, source, metricName, date, source, metricName)
    .run();
}

async function upsertGrowthTracking(db: D1Database, date: string, m: CollectedMetrics): Promise<void> {
  await db
    .prepare(
      `INSERT INTO growth_tracking
         (date, engines_count, doctrines_count, knowledge_docs, brain_messages, brain_instances, fleet_score, bot_posts_total, queries_total)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(date) DO UPDATE SET
         engines_count   = excluded.engines_count,
         doctrines_count = excluded.doctrines_count,
         knowledge_docs  = excluded.knowledge_docs,
         brain_messages  = excluded.brain_messages,
         brain_instances = excluded.brain_instances,
         fleet_score     = excluded.fleet_score,
         bot_posts_total = excluded.bot_posts_total,
         queries_total   = excluded.queries_total`,
    )
    .bind(date, m.engines, m.doctrines, m.knowledge_docs, m.brain_messages, m.brain_instances, m.fleet_score, m.bot_posts_total, m.queries)
    .run();
}

// ─── Snapshot Pipeline ────────────────────────────────────────────────────────

async function runSnapshot(env: Env): Promise<CollectedMetrics> {
  const metrics = await collectAllMetrics(env);
  const today = new Date().toISOString().slice(0, 10);

  const snapshots: MetricSnapshot[] = [
    { source: 'engine-runtime',    metric_name: 'engines',         metric_value: metrics.engines },
    { source: 'engine-runtime',    metric_name: 'doctrines',       metric_value: metrics.doctrines },
    { source: 'engine-runtime',    metric_name: 'queries',         metric_value: metrics.queries },
    { source: 'knowledge-forge',   metric_name: 'knowledge_docs',  metric_value: metrics.knowledge_docs },
    { source: 'shared-brain',      metric_name: 'brain_messages',  metric_value: metrics.brain_messages },
    { source: 'shared-brain',      metric_name: 'brain_instances', metric_value: metrics.brain_instances },
    { source: 'autonomous-daemon', metric_name: 'fleet_score',     metric_value: metrics.fleet_score },
    { source: 'autonomous-daemon', metric_name: 'daemon_cycles',   metric_value: metrics.daemon_cycles },
    { source: 'doctrine-forge',    metric_name: 'bot_posts_total', metric_value: metrics.bot_posts_total },
  ];

  await insertSnapshots(env.DB, snapshots);

  // Update daily rollups for all metric/source combos
  const rollupUpdates = snapshots.map((s) => upsertDailyRollup(env.DB, today, s.source, s.metric_name));
  await Promise.all(rollupUpdates);

  // Update growth tracking
  await upsertGrowthTracking(env.DB, today, metrics);

  // Cache current metrics for fast dashboard reads (TTL 1hr)
  await env.CACHE.put('current_metrics', JSON.stringify(metrics), { expirationTtl: 3600 });

  log('info', 'snapshot complete', { date: today, metrics_count: snapshots.length });
  return metrics;
}

// ─── Delta Helpers ────────────────────────────────────────────────────────────

function formatDelta(current: number, previous: number): string {
  const diff = current - previous;
  if (diff === 0) return '0';
  return diff > 0 ? `+${diff.toLocaleString()}` : `${diff.toLocaleString()}`;
}

function buildDelta(current: CurrentMetrics, prev: GrowthRow | null): GrowthDelta {
  if (!prev) {
    return {
      engines: 'N/A', doctrines: 'N/A', queries: 'N/A',
      knowledge_docs: 'N/A', brain_messages: 'N/A',
      fleet_score: 'N/A', daemon_cycles: 'N/A',
    };
  }
  return {
    engines:       formatDelta(current.engines,       prev.engines_count),
    doctrines:     formatDelta(current.doctrines,     prev.doctrines_count),
    queries:       formatDelta(current.queries,        prev.queries_total),
    knowledge_docs:formatDelta(current.knowledge_docs, prev.knowledge_docs),
    brain_messages:formatDelta(current.brain_messages, prev.brain_messages),
    fleet_score:   formatDelta(current.fleet_score,    prev.fleet_score),
    daemon_cycles: formatDelta(current.daemon_cycles,  prev.bot_posts_total),
  };
}

// ─── Route Handlers ───────────────────────────────────────────────────────────

async function handleHealth(env: Env): Promise<Response> {
  return json({
    ok: true,
    service: env.SERVICE_NAME,
    version: env.VERSION,
    timestamp: new Date().toISOString(),
    status: 'operational',
  });
}

async function handleDashboard(env: Env): Promise<Response> {
  // Try cache first
  const cached = await env.CACHE.get('current_metrics');
  let current: CurrentMetrics;

  if (cached) {
    const m = JSON.parse(cached) as CollectedMetrics;
    current = {
      engines: m.engines, doctrines: m.doctrines, queries: m.queries,
      knowledge_docs: m.knowledge_docs, brain_messages: m.brain_messages,
      brain_instances: m.brain_instances, fleet_score: m.fleet_score,
      daemon_cycles: m.daemon_cycles,
    };
  } else {
    const m = await collectAllMetrics(env);
    current = {
      engines: m.engines, doctrines: m.doctrines, queries: m.queries,
      knowledge_docs: m.knowledge_docs, brain_messages: m.brain_messages,
      brain_instances: m.brain_instances, fleet_score: m.fleet_score,
      daemon_cycles: m.daemon_cycles,
    };
  }

  // Fetch last 30 days of growth rows
  const { results: trends } = await env.DB.prepare(
    'SELECT * FROM growth_tracking ORDER BY date DESC LIMIT 30',
  ).all<GrowthRow>();

  const sorted = (trends ?? []).slice().reverse();

  // 24h delta: compare today vs yesterday
  const prev24h = sorted.length >= 2 ? sorted[sorted.length - 2] : null;
  // 7d delta: compare today vs 7 days ago
  const prev7d = sorted.length >= 7 ? sorted[sorted.length - 7] : null;

  const response: DashboardResponse = {
    ok: true,
    service: 'echo-analytics-engine',
    timestamp: new Date().toISOString(),
    current,
    growth_24h: buildDelta(current, prev24h),
    growth_7d:  buildDelta(current, prev7d),
    trends: sorted,
  };

  return json(response);
}

async function handleGrowth(env: Env, url: URL): Promise<Response> {
  const days = Math.min(parseInt(url.searchParams.get('days') ?? '30', 10), 90);
  const { results } = await env.DB.prepare(
    'SELECT * FROM growth_tracking ORDER BY date DESC LIMIT ?',
  )
    .bind(days)
    .all<GrowthRow>();

  return json({
    ok: true,
    days,
    count: (results ?? []).length,
    data: (results ?? []).slice().reverse(),
  });
}

async function handleMetricsBySource(env: Env, source: string, url: URL): Promise<Response> {
  const limit = Math.min(parseInt(url.searchParams.get('limit') ?? '100', 10), 500);
  const metric = url.searchParams.get('metric');

  let query: string;
  let bindings: unknown[];

  if (metric) {
    query = `SELECT * FROM metrics_snapshots WHERE source = ? AND metric_name = ? ORDER BY timestamp DESC LIMIT ?`;
    bindings = [source, metric, limit];
  } else {
    query = `SELECT * FROM metrics_snapshots WHERE source = ? ORDER BY timestamp DESC LIMIT ?`;
    bindings = [source, limit];
  }

  const stmt = env.DB.prepare(query);
  const { results } = await (bindings.length === 3
    ? stmt.bind(bindings[0], bindings[1], bindings[2])
    : stmt.bind(bindings[0], bindings[1])
  ).all<SourceMetric>();

  return json({
    ok: true,
    source,
    metric: metric ?? 'all',
    count: (results ?? []).length,
    data: results ?? [],
  });
}

async function handleSnapshot(req: Request, env: Env): Promise<Response> {
  if (!isAuthorized(req, env)) {
    return err('unauthorized', 401);
  }
  try {
    const metrics = await runSnapshot(env);
    log('info', 'manual snapshot triggered');
    return json({ ok: true, message: 'snapshot complete', metrics, timestamp: new Date().toISOString() });
  } catch (e) {
    log('error', 'snapshot failed', { error: String(e) });
    return err(`snapshot failed: ${String(e)}`, 500);
  }
}

async function handleTrends(env: Env, url: URL): Promise<Response> {
  const period = url.searchParams.get('period') ?? 'weekly';
  const weeks = period === 'monthly' ? 12 : 8; // 8 weeks or 12 months
  const days = period === 'monthly' ? weeks * 30 : weeks * 7;

  const { results: rawRows } = await env.DB.prepare(
    'SELECT * FROM growth_tracking ORDER BY date DESC LIMIT ?',
  )
    .bind(days)
    .all<GrowthRow>();

  const rows = (rawRows ?? []).slice().reverse();

  // Build weekly/monthly buckets
  interface Bucket {
    period: string;
    engines: number;
    doctrines: number;
    queries: number;
    knowledge_docs: number;
    brain_messages: number;
    fleet_score: number;
    rows: number;
  }

  const buckets = new Map<string, Bucket>();

  for (const row of rows) {
    const d = new Date(row.date);
    let key: string;
    if (period === 'monthly') {
      key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    } else {
      // ISO week number
      const jan1 = new Date(d.getFullYear(), 0, 1);
      const week = Math.ceil(((d.getTime() - jan1.getTime()) / 86400000 + jan1.getDay() + 1) / 7);
      key = `${d.getFullYear()}-W${String(week).padStart(2, '0')}`;
    }

    const existing = buckets.get(key);
    if (!existing) {
      buckets.set(key, {
        period: key,
        engines: row.engines_count,
        doctrines: row.doctrines_count,
        queries: row.queries_total,
        knowledge_docs: row.knowledge_docs,
        brain_messages: row.brain_messages,
        fleet_score: row.fleet_score,
        rows: 1,
      });
    } else {
      // Take the latest value in each bucket (already sorted asc)
      existing.engines = row.engines_count;
      existing.doctrines = row.doctrines_count;
      existing.queries = row.queries_total;
      existing.knowledge_docs = row.knowledge_docs;
      existing.brain_messages = row.brain_messages;
      existing.fleet_score = row.fleet_score;
      existing.rows += 1;
    }
  }

  const bucketList = Array.from(buckets.values());

  // Calculate period-over-period growth rates
  const withGrowth = bucketList.map((b, i) => {
    if (i === 0) return { ...b, doctrines_growth_pct: null, queries_growth_pct: null };
    const prev = bucketList[i - 1];
    const doctrinesGrowth =
      prev.doctrines > 0 ? Math.round(((b.doctrines - prev.doctrines) / prev.doctrines) * 10000) / 100 : null;
    const queriesGrowth =
      prev.queries > 0 ? Math.round(((b.queries - prev.queries) / prev.queries) * 10000) / 100 : null;
    return { ...b, doctrines_growth_pct: doctrinesGrowth, queries_growth_pct: queriesGrowth };
  });

  return json({
    ok: true,
    period,
    bucket_count: withGrowth.length,
    data: withGrowth,
  });
}

// ─── Cron Handler ─────────────────────────────────────────────────────────────

async function handleCron(env: Env): Promise<void> {
  log('info', 'cron triggered — running scheduled snapshot');
  try {
    await runSnapshot(env);
  } catch (e) {
    log('error', 'cron snapshot failed', { error: String(e) });
  }
}

// ─── Security Headers ─────────────────────────────────────────────────────────

function addSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set('X-Content-Type-Options', 'nosniff');
  headers.set('X-Frame-Options', 'DENY');
  headers.set('X-XSS-Protection', '1; mode=block');
  headers.set('Strict-Transport-Security', 'max-age=63072000; includeSubDomains; preload');
  headers.set('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

// ─── Router ───────────────────────────────────────────────────────────────────

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const { pathname } = url;
    const method = req.method;

    // OPTIONS preflight
    if (method === 'OPTIONS') {
      return addSecurityHeaders(new Response(null, { status: 204, headers: CORS_HEADERS }));
    }

    log('info', 'request', { method, path: pathname });

    try {
      // GET / — root info
      if (method === 'GET' && (pathname === '/' || pathname === '')) {
        return addSecurityHeaders(json({
          ok: true,
          service: env.SERVICE_NAME,
          version: env.VERSION,
          endpoints: ['/health', '/dashboard', '/growth', '/metrics/:source', '/snapshot', '/trends'],
          timestamp: new Date().toISOString(),
        }));
      }

      // GET /health
      if (method === 'GET' && pathname === '/health') {
        return addSecurityHeaders(await handleHealth(env));
      }

      // GET /dashboard
      if (method === 'GET' && pathname === '/dashboard') {
        return addSecurityHeaders(await handleDashboard(env));
      }

      // GET /growth
      if (method === 'GET' && pathname === '/growth') {
        return addSecurityHeaders(await handleGrowth(env, url));
      }

      // GET /metrics/:source
      const metricsMatch = pathname.match(/^\/metrics\/([^/]+)$/);
      if (method === 'GET' && metricsMatch) {
        return addSecurityHeaders(await handleMetricsBySource(env, decodeURIComponent(metricsMatch[1]), url));
      }

      // POST /snapshot
      if (method === 'POST' && pathname === '/snapshot') {
        return addSecurityHeaders(await handleSnapshot(req, env));
      }

      // GET /trends
      if (method === 'GET' && pathname === '/trends') {
        return addSecurityHeaders(await handleTrends(env, url));
      }

      return addSecurityHeaders(err('not found', 404));
    } catch (e) {
      log('error', 'unhandled error', { path: pathname, error: String(e) });
      return addSecurityHeaders(err(`internal error: ${String(e)}`, 500));
    }
  },

  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(handleCron(env));
  },
} satisfies ExportedHandler<Env>;
