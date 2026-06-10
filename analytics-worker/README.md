# Unreeled analytics collector

Privacy-minimal first-party event collector for Unreeled. It stores event names,
coarse metadata, page paths, device classes, and timestamps in Cloudflare D1.
It does not collect search terms, release titles, account details, or IP
addresses.

Query recent totals:

```powershell
npx wrangler d1 execute unreeled-analytics --remote --command "SELECT event_name, COUNT(*) AS total FROM analytics_events GROUP BY event_name ORDER BY total DESC"
```

Query MerchShake clicks by placement:

```powershell
npx wrangler d1 execute unreeled-analytics --remote --command "SELECT json_extract(properties, '$.placement') AS placement, COUNT(*) AS clicks FROM analytics_events WHERE event_name = 'merchshake_click' GROUP BY placement ORDER BY clicks DESC"
```

Query daily MerchShake clicks:

```powershell
npx wrangler d1 execute unreeled-analytics --remote --command "SELECT date(created_at) AS day, COUNT(*) AS clicks FROM analytics_events WHERE event_name = 'merchshake_click' GROUP BY day ORDER BY day DESC LIMIT 30"
```
