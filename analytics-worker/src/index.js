const ALLOWED_EVENTS = new Set([
  "filter_used",
  "merchshake_click",
  "search_used",
  "section_reached",
  "signup_completed",
  "signup_started",
  "signup_submitted",
  "watchlist_attempt",
]);

const ALLOWED_PROPERTIES = new Set([
  "action",
  "authenticated",
  "filter",
  "has_query",
  "media_type",
  "method",
  "placement",
  "result_bucket",
  "section",
  "source",
  "value",
]);

function corsHeaders(origin) {
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin",
  };
}

function allowedOrigin(origin) {
  if (origin === "https://unreeled.co.za") return true;
  return /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin);
}

function cleanString(value, maxLength = 80) {
  return String(value ?? "").replace(/[\u0000-\u001f\u007f]/g, "").slice(0, maxLength);
}

function cleanProperties(properties) {
  const clean = {};
  if (!properties || typeof properties !== "object" || Array.isArray(properties)) return clean;

  for (const [key, value] of Object.entries(properties)) {
    if (!ALLOWED_PROPERTIES.has(key)) continue;
    if (typeof value === "boolean") clean[key] = value;
    else if (typeof value === "string" || typeof value === "number") clean[key] = cleanString(value);
  }
  return clean;
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get("Origin") || "";
    if (!allowedOrigin(origin)) return new Response("Forbidden", { status: 403 });

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (request.method !== "POST" || new URL(request.url).pathname !== "/event") {
      return new Response("Not found", { status: 404, headers: corsHeaders(origin) });
    }

    if (Number(request.headers.get("Content-Length") || 0) > 4096) {
      return new Response("Payload too large", { status: 413, headers: corsHeaders(origin) });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("Invalid JSON", { status: 400, headers: corsHeaders(origin) });
    }

    const eventName = cleanString(body?.event, 40);
    if (!ALLOWED_EVENTS.has(eventName)) {
      return new Response("Invalid event", { status: 400, headers: corsHeaders(origin) });
    }

    const page = cleanString(body?.page || "/", 120);
    const device = ["mobile", "tablet", "desktop"].includes(body?.device) ? body.device : "unknown";
    const properties = JSON.stringify(cleanProperties(body?.properties));

    await env.ANALYTICS_DB.prepare(
      "INSERT INTO analytics_events (event_name, page, device, properties) VALUES (?, ?, ?, ?)"
    ).bind(eventName, page, device, properties).run();

    return new Response(null, { status: 204, headers: corsHeaders(origin) });
  },
};
