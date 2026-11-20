// Shared ESI helpers.
//
// CCP's API is public for market data - no auth, no key. The rate limit is
// 12,000 requests per 15 minutes per IP, which is generous, but the error
// limit is the one that actually bites: burn through it and you get a
// temporary ban. So every fetch watches X-Esi-Error-Limit-Remain and backs
// off before CCP has to.

export const ESI = "https://esi.evetech.net/latest";
export const UA = { "User-Agent": "EveNYX/1.0 (market research, non-commercial)" };

// The Forge is Jita, which is roughly 80% of all EVE trade.
export const DEFAULT_REGION = 10000002;

let errorBudget = 100;

export async function esi(path, { method = "GET", body = null, tries = 3 } = {}) {
  for (let attempt = 0; attempt < tries; attempt++) {
    // If CCP says we are near the error limit, wait it out rather than push.
    if (errorBudget < 10) {
      await sleep(2000);
      errorBudget = 100;
    }
    const res = await fetch(`${ESI}${path}`, {
      method,
      headers: body ? { ...UA, "Content-Type": "application/json" } : UA,
      body: body ? JSON.stringify(body) : undefined,
    });

    const remain = res.headers.get("x-esi-error-limit-remain");
    if (remain != null) errorBudget = Number(remain);

    if (res.ok) {
      return { data: await res.json(), pages: Number(res.headers.get("x-pages") || 1) };
    }
    // 420 is ESI's "you have been error-limited" status.
    if (res.status === 420 || res.status === 503 || res.status === 500) {
      await sleep(1000 * (attempt + 1));
      continue;
    }
    if (res.status === 404) return { data: null, pages: 0, notFound: true };
    throw new Error(`esi ${res.status} on ${path}`);
  }
  throw new Error(`esi gave up after ${tries} tries on ${path}`);
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Run jobs with bounded concurrency. ESI is fast, but firing 400 requests at
// once is how you find the error limit.
export async function pool(items, limit, worker) {
  const out = [];
  let i = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const n = i++;
      out[n] = await worker(items[n], n);
    }
  });
  await Promise.all(runners);
  return out;
}
