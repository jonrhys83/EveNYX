// Daily snapshot of the Jita order book, aggregated per item type.
//
// This is the thing RuneScape does not publish: a real bid and ask with depth
// behind each. ~412,000 live orders across 413 pages, pulled in well under a
// minute, collapsed to one row per type.
//
// It also decides which types are "tracked" - the top N by ISK resting on the
// book. The top 1,000 carry over 99% of the value, so storing all 16,000 would
// cost 16x the space for the last 0.8%.

import { requireEnv, insert, rpc, log } from "./_lib.js";
import { esi, pool, DEFAULT_REGION } from "./_esi.js";

const TRACK_TOP = Number(process.env.EVE_TRACK_TOP || 1000);
const CONCURRENCY = 12;

// Jita IV - Moon 4 - Caldari Navy Assembly Plant. Aggregating across the whole
// region mixes 23 stations and yields nonsense: a best bid ABOVE the best ask,
// because an order 10 jumps away is not one you can trade against. This single
// station carries ~75% of the region's orders and is what players mean by
// "Jita price".
const STATION = Number(process.env.EVE_STATION || 60003760);

export default async (req) => {
  const started = Date.now();
  try {
    requireEnv();
    const region = Number(process.env.EVE_REGION || DEFAULT_REGION);

    // Page 1 tells us how many pages there are.
    const first = await esi(`/markets/${region}/orders/?order_type=all&page=1`);
    const pages = first.pages;
    const orders = [...first.data];

    const rest = Array.from({ length: pages - 1 }, (_, i) => i + 2);
    const more = await pool(rest, CONCURRENCY, async (p) => {
      const r = await esi(`/markets/${region}/orders/?order_type=all&page=${p}`);
      return r.data || [];
    });
    for (const chunk of more) orders.push(...chunk);
    log(`fetched ${orders.length} orders across ${pages} pages`);

    // Collapse to best bid / best ask / depth per type.
    const agg = new Map();
    let atStation = 0;
    for (const o of orders) {
      if (STATION && o.location_id !== STATION) continue;
      atStation++;
      let e = agg.get(o.type_id);
      if (!e) {
        e = { bid: 0, ask: Infinity, bidVol: 0, askVol: 0, n: 0 };
        agg.set(o.type_id, e);
      }
      e.n++;
      if (o.is_buy_order) {
        if (o.price > e.bid) e.bid = o.price;
        e.bidVol += o.volume_remain;
      } else {
        if (o.price < e.ask) e.ask = o.price;
        e.askVol += o.volume_remain;
      }
    }

    const snapDate = new Date().toISOString().slice(0, 10);
    const rows = [];
    const ranked = [];
    for (const [type_id, e] of agg) {
      const bid = e.bid > 0 ? e.bid : null;
      const ask = e.ask < Infinity ? e.ask : null;
      rows.push({
        type_id, region_id: region, station_id: STATION || null, snap_date: snapDate,
        best_bid: bid, best_ask: ask,
        bid_volume: e.bidVol, ask_volume: e.askVol,
        order_count: e.n,
        spread_pct: bid && ask ? +(((ask - bid) / ask) * 100).toFixed(3) : null,
      });
      if (bid && ask) ranked.push({ type_id, isk: ask * e.askVol, orders: e.n });
    }

    // Store the book in batches - 16k rows in one POST is a large body.
    let stored = 0;
    for (let i = 0; i < rows.length; i += 2000) {
      stored += await insert("eve_book", rows.slice(i, i + 2000),
        { onConflict: "type_id,region_id,snap_date" });
    }
    log(`stored ${stored} book rows for ${snapDate} (${atStation} orders at station ${STATION})`);

    // Refresh the tracked set.
    //
    // Ranking by ISK resting on the book alone is wrong: it favours expensive
    // items with deep ask walls and drops cheap high-turnover ones. Tritanium,
    // the single most traded item in EVE, came 1099th by ISK and 71st by order
    // count. Only ~267 items land in both top-1000 lists, so take the union:
    // order count as the liquidity proxy, ISK on book to catch the high-value
    // items that trade in small numbers.
    const byOrders = [...ranked].sort((a, b) => b.orders - a.orders)
      .slice(0, TRACK_TOP).map((r) => r.type_id);
    const byIsk = [...ranked].sort((a, b) => b.isk - a.isk)
      .slice(0, Math.floor(TRACK_TOP / 2)).map((r) => r.type_id);
    const top = [...new Set([...byOrders, ...byIsk])];

    // The hot tier: the most actively quoted items, refreshed every run.
    // Everything else rotates (see eve-history-background).
    const HOT_TOP = Number(process.env.EVE_HOT_TOP || 250);
    const hot = [...ranked].sort((a, b) => b.orders - a.orders)
      .slice(0, HOT_TOP).map((r) => r.type_id);
    await rpc("eve_set_tracked", { ids: top, hot_ids: hot });
    log(`tracked set refreshed: ${top.length} types, ${hot.length} hot`);

    const seconds = +((Date.now() - started) / 1000).toFixed(1);
    return new Response(JSON.stringify({
      ok: true, snapDate, orders: orders.length, atStation, pages,
      types: rows.length, tracked: top.length, hot: hot.length, seconds,
    }), { headers: { "content-type": "application/json" } });
  } catch (e) {
    log("EVE BOOK FAILED:", e.message);
    return new Response(JSON.stringify({ ok: false, error: e.message }), {
      status: 500, headers: { "content-type": "application/json" },
    });
  }
};
