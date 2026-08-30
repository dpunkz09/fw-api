/**
 * VidNest Decrypt Proxy + Upstash Redis Cache
 *
 * Express 5 / Node.js 22+
 */

import "dotenv/config";
import express from "express";
import Redis from "ioredis";
import crypto from "node:crypto";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const PORT        = process.env.PORT        || 3000;
const BASE_API    = process.env.BASE_API    || "https://new.vidnest.fun";
const CACHE_TTL   = Number(process.env.CACHE_TTL   || 3600);
const FETCH_TIMEOUT = Number(process.env.FETCH_TIMEOUT || 15000);

// Custom base64 alphabet used by VidNest
const ALPHABET    = process.env.VIDNEST_ALPHABET ||
  "RB0fpH8ZEyVLkv7c2i6MAJ5u3IKFDxlS1NTsnGaqmXYdUrtzjwObCgQP94hoeW+/=";

// Upstream request headers
const UPSTREAM_UA       = process.env.UPSTREAM_UA      || "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36";
const UPSTREAM_REFERER  = process.env.UPSTREAM_REFERER || "https://vidnest.fun/";
const UPSTREAM_ORIGIN   = process.env.UPSTREAM_ORIGIN  || "https://vidnest.fun";

// HLS reverse proxy — wraps HLS stream URLs so the player fetches segments
// through this proxy instead of hitting the origin directly.
// Set to empty string to disable.
const HLS_PROXY = process.env.HLS_PROXY ?? "https://proxy.jpaworx.com/?url=";

// ---------------------------------------------------------------------------
// Validate required env
// ---------------------------------------------------------------------------

if (!process.env.REDIS_URL) {
  console.error("ERROR: REDIS_URL is not configured.");
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Redis
// ---------------------------------------------------------------------------

const redis = new Redis(process.env.REDIS_URL, {
  maxRetriesPerRequest: 2,
  retryStrategy: (times) => Math.min(times * 100, 3000),
});

redis.on("connect", () => console.log("Redis: connecting..."));
redis.on("ready",   () => console.log("Redis: connected and ready"));
redis.on("error",   (err) => console.error("Redis error:", err.message));
redis.on("close",   () => console.log("Redis connection closed"));

// ---------------------------------------------------------------------------
// In-flight deduplication
// Prevents multiple simultaneous identical upstream requests (cache stampede).
// ---------------------------------------------------------------------------

/** @type {Map<string, Promise<object>>} */
const inFlight = new Map();

// ---------------------------------------------------------------------------
// Custom Base64 decoder
// ---------------------------------------------------------------------------

// Build lookup once at startup instead of inside every call
const BASE64_LOOKUP = Object.fromEntries(
  [...ALPHABET].map((ch, i) => [ch, i])
);
const BASE64_PAD = ALPHABET[64];

function customBase64Decode(input) {
  const bytes = [];

  for (let i = 0; i < input.length; i += 4) {
    const c0 = input[i]     ?? BASE64_PAD;
    const c1 = input[i + 1] ?? BASE64_PAD;
    const c2 = input[i + 2] ?? BASE64_PAD;
    const c3 = input[i + 3] ?? BASE64_PAD;

    const v0 = BASE64_LOOKUP[c0] ?? 0;
    const v1 = BASE64_LOOKUP[c1] ?? 0;
    const v2 = BASE64_LOOKUP[c2] ?? 64;
    const v3 = BASE64_LOOKUP[c3] ?? 64;

    bytes.push((v0 << 2) | (v1 >> 4));
    if (v2 !== 64) bytes.push(((v1 & 0xf) << 4) | (v2 >> 2));
    if (v3 !== 64) bytes.push(((v2 & 0x3) << 6) | v3);
  }

  return Buffer.from(bytes).toString("utf-8");
}

// ---------------------------------------------------------------------------
// Decrypt
// ---------------------------------------------------------------------------

function decrypt(encryptedString) {
  const text = customBase64Decode(encryptedString);
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

// ---------------------------------------------------------------------------
// HLS proxy wrapper
// ---------------------------------------------------------------------------

/**
 * Returns true if the URL or type indicates an HLS stream.
 */
function isHls(url, type) {
  return type === "hls" || (typeof url === "string" && url.includes(".m3u8"));
}

/**
 * Wraps an HLS URL with the configured proxy.
 * Falls through unchanged if HLS_PROXY is empty or url is falsy.
 */
function wrapHlsUrl(url) {
  if (!HLS_PROXY || !url) return url;
  return `${HLS_PROXY}${encodeURIComponent(url)}`;
}

// ---------------------------------------------------------------------------
// Extract URLs
// ---------------------------------------------------------------------------

function extractUrls(data) {
  if (typeof data === "string") return { url: data };

  if (!data || typeof data !== "object") {
    return { error: "unexpected type", raw: data };
  }

  const result = {};

  // Direct url field
  if (data.url) {
    const hlsDetected = data.hls || isHls(data.url, null);
    result.url = hlsDetected ? wrapHlsUrl(data.url) : data.url;
    if (data.headers) result.headers  = data.headers;
    if (hlsDetected)  result.type     = "hls";
    if (data.referer) result.referer  = data.referer;
  }

  // Nested playlist (always HLS)
  if (data.data?.stream?.playlist) {
    result.url = wrapHlsUrl(data.data.stream.playlist);
    if (data.headers) result.headers = data.headers;
  }

  // streams[] — wrap each HLS entry's url, then promote to result.url
  if (Array.isArray(data.streams) && data.streams.length) {
    result.streams = data.streams
      .filter((s) => s?.url)
      .map((s) => {
        const hlsStream = isHls(s.url, s.type);
        return {
          url:      hlsStream ? wrapHlsUrl(s.url) : s.url,
          type:     s.type     ?? null,
          language: s.language ?? s.quality ?? null,
          headers:  s.headers  ?? null,
        };
      });

    if (!result.url) {
      const hls      = result.streams.find((s) => s.type === "hls" || s.url?.includes(encodeURIComponent(".m3u8")) || s.url?.includes(".m3u8"));
      const fallback = result.streams[0];
      result.url = (hls ?? fallback)?.url ?? null;
    }
  }

  // sources[] — wrap HLS, promote to result.url
  if (Array.isArray(data.sources) && data.sources.length) {
    result.sources = data.sources.map((s) => {
      if (!s?.url) return s;
      const hlsSource = isHls(s.url, s.type);
      return hlsSource ? { ...s, url: wrapHlsUrl(s.url) } : s;
    });

    if (!result.url) {
      const hls = result.sources.find((s) => isHls(s.url, s.type));
      result.url = hls?.url ?? result.sources[0]?.url ?? null;
    }
  }

  if (data.title) result.title = data.title;

  // all_urls — wrap each entry through HLS proxy (zeta/nextgencloudfabric)
  if (Array.isArray(data.all_urls) && data.all_urls.length) {
    result.all_urls = data.all_urls.map((u) =>
      isHls(u, null) ? wrapHlsUrl(u) : u
    );
  }

  return result;
}

// ---------------------------------------------------------------------------
// Redis helpers
// ---------------------------------------------------------------------------

function getCacheKey(upstream) {
  const hash = crypto.createHash("sha256").update(upstream).digest("hex");
  return `vidnest:stream:${hash}`;
}

/**
 * Write a result object to Redis. Swallows errors so cache failures
 * never break the proxy.
 */
async function cacheSet(key, result, path) {
  try {
    await redis.set(key, JSON.stringify(result), "EX", CACHE_TTL);
    console.log(`[CACHE SET] ${path} TTL=${CACHE_TTL}s`);
  } catch (err) {
    console.error("[REDIS SET ERROR]", err.message);
  }
}

// ---------------------------------------------------------------------------
// Upstream fetch with timeout
// ---------------------------------------------------------------------------

async function fetchUpstream(url, headers) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), FETCH_TIMEOUT);

  try {
    return await fetch(url, { headers, signal: ac.signal });
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// Core stream resolver
// Fetches upstream, decrypts, extracts, caches — returns a result object.
// ---------------------------------------------------------------------------

async function resolveStream(serverPath, cacheKey) {
  const upstream = `${BASE_API}/${serverPath}`;

  const headers = {
    "User-Agent":      UPSTREAM_UA,
    "Referer":         UPSTREAM_REFERER,
    "Origin":          UPSTREAM_ORIGIN,
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
  };

  console.log(`[UPSTREAM] ${upstream}`);

  let resp = await fetchUpstream(upstream, headers);

  // Retry 403 with alternate referer
  if (resp.status === 403) {
    console.log(`[UPSTREAM 403] Retrying ${serverPath}`);
    const altHeaders = { ...headers, Referer: `${BASE_API}/` };
    delete altHeaders.Origin;
    resp = await fetchUpstream(upstream, altHeaders);
  }

  if (!resp.ok) {
    const body = await resp.text();
    console.error(`[UPSTREAM ERROR] ${resp.status} ${serverPath}`);
    // Return an error-shaped object (not cached)
    return {
      ok:       false,
      error:    `Upstream returned ${resp.status}`,
      upstream,
      body,
      _status:  resp.status, // stripped before response
    };
  }

  const contentType = resp.headers.get("content-type") || "";

  // Non-JSON response (e.g. raw m3u8 text)
  if (!contentType.includes("application/json")) {
    const text = await resp.text();
    return {
      ok:       true,
      upstream,
      extracted: extractUrls(text),
      raw:       text,
    };
  }

  const json = await resp.json();

  // VidNest encrypted response
  if (typeof json.data === "string") {
    const decrypted = decrypt(json.data);
    return {
      ok:       true,
      upstream,
      extracted: extractUrls(decrypted),
      raw:       decrypted,
    };
  }

  // Plain JSON response
  return {
    ok:       true,
    upstream,
    extracted: extractUrls(json),
    raw:       json,
  };
}

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------

const app = express();

// ---------------------------------------------------------------------------
// GET /decrypt/:encoded
// ---------------------------------------------------------------------------

app.get("/decrypt/:encoded", (req, res) => {
  const { encoded } = req.params;

  if (!encoded) {
    return res.status(400).json({ ok: false, error: "No encrypted string provided." });
  }

  try {
    const decrypted = decrypt(encoded);
    return res.json({ ok: true, extracted: extractUrls(decrypted), raw: decrypted });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /stream/{*path}
// ---------------------------------------------------------------------------

app.get("/stream/{*path}", async (req, res) => {
  let serverPath = req.params.path;
  if (Array.isArray(serverPath)) serverPath = serverPath.join("/");

  if (!serverPath) {
    return res.status(400).json({ ok: false, error: "Missing stream path." });
  }

  const upstream = `${BASE_API}/${serverPath}`;
  const cacheKey = getCacheKey(upstream);

  // ── Cache check ──────────────────────────────────────────────────────────

  try {
    const cached = await redis.get(cacheKey);
    if (cached) {
      console.log(`[CACHE HIT] ${serverPath}`);
      return res.json({ ...JSON.parse(cached), cached: true });
    }
    console.log(`[CACHE MISS] ${serverPath}`);
  } catch (err) {
    console.error("[REDIS GET ERROR]", err.message);
  }

  // ── In-flight deduplication ───────────────────────────────────────────────
  // If another request for the same path is already in progress, wait for it
  // instead of firing a duplicate upstream request.

  if (inFlight.has(cacheKey)) {
    console.log(`[IN-FLIGHT] ${serverPath}`);
    try {
      const result = await inFlight.get(cacheKey);
      // The in-flight request already wrote to cache; just return its result.
      const { _status, ...payload } = result;
      return res.status(_status ?? 200).json(payload);
    } catch (err) {
      return res.status(500).json({ ok: false, error: err.message, upstream });
    }
  }

  // ── Upstream fetch ────────────────────────────────────────────────────────

  const promise = resolveStream(serverPath, cacheKey);
  inFlight.set(cacheKey, promise);

  let result;
  try {
    result = await promise;
  } catch (err) {
    inFlight.delete(cacheKey);
    console.error(`[STREAM ERROR] ${serverPath}`, err.message);
    return res.status(500).json({ ok: false, error: err.message, upstream });
  }

  inFlight.delete(cacheKey);

  // Cache only successful results
  if (result.ok) {
    await cacheSet(cacheKey, result, serverPath);
  }

  const { _status, ...payload } = result;
  return res.status(_status ?? 200).json(payload);
});

// ---------------------------------------------------------------------------
// GET /cache/status
// ---------------------------------------------------------------------------

app.get("/cache/status", async (req, res) => {
  try {
    const pong = await redis.ping();
    return res.json({ ok: true, redis: pong, cache_ttl: CACHE_TTL });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ---------------------------------------------------------------------------
// DELETE /cache/:encoded  — clear a single cached entry by stream path
// GET    /cache/clear     — flush ALL vidnest:stream:* keys
// ---------------------------------------------------------------------------

app.delete("/cache/:encoded", async (req, res) => {
  const { encoded } = req.params;
  const upstream    = `${BASE_API}/${decodeURIComponent(encoded)}`;
  const cacheKey    = getCacheKey(upstream);

  try {
    const deleted = await redis.del(cacheKey);
    return res.json({ ok: true, deleted: deleted > 0, key: cacheKey });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/cache/clear", async (req, res) => {
  try {
    // SCAN instead of KEYS to avoid blocking Redis on large datasets
    let cursor = "0";
    let total  = 0;

    do {
      const [next, keys] = await redis.scan(cursor, "MATCH", "vidnest:stream:*", "COUNT", 100);
      cursor = next;
      if (keys.length) {
        await redis.del(...keys);
        total += keys.length;
      }
    } while (cursor !== "0");

    return res.json({ ok: true, deleted: total });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /
// ---------------------------------------------------------------------------

app.get("/", (_req, res) => {
  res.json({
    name: "VidNest Decrypt Proxy",
    routes: {
      "GET /decrypt/:encoded":   "Decrypt a VidNest custom-b64 string",
      "GET /stream/*path":       "Fetch + decrypt from upstream on-the-fly",
      "GET /cache/status":       "Redis ping + current TTL",
      "GET /cache/clear":        "Flush all vidnest:stream:* cache keys",
      "DELETE /cache/:encoded":  "Delete a single cache entry by stream path",
    },
    servers: {
      catflix: "/stream/buzz/movie/{tmdbId}",
      lamda:   "/stream/allmovies/movie/{tmdbId}",
      hexa:    "/stream/vidlink/movie/{tmdbId}",
      ophim:   "/stream/klikxxi/movie/{tmdbId}",
      beta:    "/stream/vidxyz/movie/{tmdbId}",
      sigma:   "/stream/hollymoviehd/movie/{tmdbId}",
      gama:    "/stream/vidzee/movie/{tmdbId}",
      alfa:    "/stream/videasy/movie/{tmdbId}",
      filxer:  "/stream/rogflix/movie/{tmdbId}",
      prime:   "/stream/hollymoviehd/movie/{tmdbId}",
      zeta:    "/stream/nextgencloudfabric/movie/{tmdbId}",
    },
    example: "/stream/buzz/movie/533535",
  });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`VidNest Proxy running on http://localhost:${PORT}`);
  console.log(`Base API   : ${BASE_API}`);
  console.log(`Cache TTL  : ${CACHE_TTL}s`);
  console.log(`Fetch timeout: ${FETCH_TIMEOUT}ms`);
});
