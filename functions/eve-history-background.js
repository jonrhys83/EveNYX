// Resolves type names and backfills ESI's daily market history.
//
// ESI serves ~414 days of history per type per region, which is the whole
// reason EVE is a better subject than RuneScape: a backtest exists on day one
// instead of three months from now.
//
// One request per type, so the tracked set of 1,000 is ~1,000 requests. That
// fits inside the rate limit and inside a background function, but to stay
// well clear of both it works in resumable chunks and records its position in
// eve_sync. Re-run it until done is reported true.
//
//   curl -X POST .../eve-history-background            # next chunk
//   curl -X POST .../eve-history-background -d '{"restart":true}'

import { requireEnv, insert, log, SUPABASE_URL, SERVICE_KEY } from "./_lib.js";
import { esi, pool, DEFAULT_REGION } from "./_esi.js";

const CHUNK = Number(process.env.EVE_HISTORY_CHUNK || 300);
const CONCURRENCY = 8;

async function sel(path) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` },
  });
  if (!res.ok) throw new Error(`select ${res.status}: ${(await res.text()).slice(0, 200)}`);
  return res.json();
}

// ESI rejects the whole batch if any id is unresolvable, so keep batches small
// and drop a batch that fails rather than losing the run.
async function resolveNames(ids) {
  const out = [];
  for (let i = 0; i < ids.length; i += 200) {
    const batch = ids.slice(i, i + 200);
    try {
      const r = await esi("/universe/names/", { method: "POST", body: batch, tries: 2 });
      for (const n of r.data || []) {
        if (n.category === "inventory_type") out.push({ type_id: n.id, name: n.name });
      }
    } catch {
      log(`name batch of ${batch.length} failed, skipping`);
    }
  }
  return out;
}

export default async (req) => {
  const started = Date.now();
  try {
    requireEnv();
    const region = Number(process.env.EVE_REGION || DEFAULT_REGION);
    let restart = false;
    try { restart = (await req.json())?.restart === true; } catch { /* no body */ }

    // --- names for anything tracked that lacks one ---
    const unnamed = await sel(
      "eve_types?select=type_id&tracked=is.true&name=is.null&limit=1000");
    if (unnamed.length) {
      const named = await resolveNames(unnamed.map((r) => r.type_id));
      if (named.length) await insert("eve_types", named, { onConflict: "type_id" });
      log(`named ${named.length} of ${unnamed.length} types`);
    }

    // --- resumable history backfill over the tracked set ---
    // PostgREST caps a response at 1000 rows no matter what limit you ask for,
    // so a single request silently truncates the tracked set. Page through it.
    const tracked = [];
    for (let off = 0; ; off += 1000) {
      const page = await sel(
        `eve_types?select=type_id&tracked=is.true&order=type_id.asc&limit=1000&offset=${off}`);
      tracked.push(...page.map((r) => r.type_id));
      if (page.length < 1000) break;
      if (off > 20000) break;   // safety stop
    }

    // Hot tier: always refreshed, every run.
    const hot = (await sel(
      "eve_types?select=type_id&hot=is.true&order=type_id.asc&limit=1000"))
      .map((r) => r.type_id);
    const hotSet = new Set(hot);

    // Cold tier: everything else, rotated on a wrapping cursor so the whole
    // set is covered every few days without refetching it all daily.
    const cold = tracked.filter((t) => !hotSet.has(t));

    const syncRows = await sel("eve_sync?select=*&job=eq.history");
    let cursor = restart ? 0 : (syncRows[0]?.cursor_pos ?? 0);
    if (cursor >= cold.length) cursor = 0;          // wrap
    const coldSlice = cold.slice(cursor, cursor + CHUNK);
    const slice = [...hot, ...coldSlice];

    if (!slice.length) {
      await insert("eve_sync", [{
        job: "history", cursor_pos: 0, total: tracked.length,
        last_run: new Date().toISOString(), last_note: "nothing tracked",
      }], { onConflict: "job" });
      return new Response(JSON.stringify({ ok: true, done: true, tracked: 0 }),
        { headers: { "content-type": "application/json" } });
    }

    let rowsWritten = 0, missing = 0;
    const results = await pool(slice, CONCURRENCY, async (type_id) => {
      const r = await esi(`/markets/${region}/history/?type_id=${type_id}`, { tries: 2 });
      if (!r.data || !r.data.length) { missing++; return []; }
      return r.data.map((d) => ({
        type_id, region_id: region, hist_date: d.date,
        average: d.average, highest: d.highest, lowest: d.lowest,
        volume: d.volume, order_count: d.order_count,
      }));
    });

    const flat = results.flat();
    for (let i = 0; i < flat.length; i += 3000) {
      rowsWritten += await insert("eve_history", flat.slice(i, i + 3000),
        { onConflict: "type_id,region_id,hist_date" });
    }

    cursor += coldSlice.length;
    const lapComplete = cursor >= cold.length;
    await insert("eve_sync", [{
      job: "history", cursor_pos: lapComplete ? 0 : cursor, total: tracked.length,
      last_run: new Date().toISOString(),
      last_note: lapComplete
        ? `lap complete (${cold.length} cold + ${hot.length} hot)`
        : `cold ${cursor}/${cold.length}, hot ${hot.length}`,
    }], { onConflict: "job" });

    const seconds = +((Date.now() - started) / 1000).toFixed(1);
    log(`history: ${rowsWritten} rows for ${slice.length} types ` +
        `(${hot.length} hot + cold ${cursor}/${cold.length}), ${missing} had none`);

    return new Response(JSON.stringify({
      ok: true, done: lapComplete, cursor, cold: cold.length, hot: hot.length,
      total: tracked.length, types: slice.length, rows: rowsWritten, missing, seconds,
    }), { headers: { "content-type": "application/json" } });
  } catch (e) {
    log("EVE HISTORY FAILED:", e.message);
    return new Response(JSON.stringify({ ok: false, error: e.message }), {
      status: 500, headers: { "content-type": "application/json" },
    });
  }
};
