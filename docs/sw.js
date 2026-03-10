const STATIC_CACHE = 'unreeled-static-v6';
const DATA_CACHE = 'unreeled-data-v6';
const IMAGE_CACHE = 'unreeled-images-v6';
const STATIC_ASSETS = ['/', '/index.html', '/manifest.json', '/feed.xml'];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then(cache => cache.addAll(STATIC_ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => ![STATIC_CACHE, DATA_CACHE, IMAGE_CACHE].includes(k))
          .map(k => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  const { request } = event;
  if (request.method !== 'GET') return;
  if (request.url.includes('supabase.co')) return;

  const url = new URL(request.url);

  if (url.pathname.startsWith('/data/')) {
    event.respondWith((async () => {
      const cache = await caches.open(DATA_CACHE);
      const cached = await cache.match(request);
      const network = fetch(request)
        .then(response => {
          if (response.ok) cache.put(request, response.clone());
          return response;
        })
        .catch(() => null);
      if (cached) {
        event.waitUntil(network);
        return cached;
      }
      return (await network) || caches.match('/index.html');
    })());
    return;
  }

  if (request.destination === 'image') {
    event.respondWith((async () => {
      const cache = await caches.open(IMAGE_CACHE);
      const cached = await cache.match(request);
      if (cached) return cached;
      try {
        const response = await fetch(request);
        if (response.ok) cache.put(request, response.clone());
        return response;
      } catch {
        return caches.match('/og-image.png');
      }
    })());
    return;
  }

  event.respondWith(
    fetch(request)
      .then(response => {
        if (response.ok && (url.pathname === '/' || url.pathname === '/index.html')) {
          caches.open(STATIC_CACHE).then(cache => cache.put(request, response.clone()));
        }
        return response;
      })
      .catch(() => caches.match(request).then(r => r || caches.match('/index.html')))
  );
});

self.addEventListener('push', event => {
  const data = event.data ? event.data.json() : {};
  const title = data.title || 'UNREELED';
  const options = {
    body: data.body || 'New releases available!',
    icon: data.icon || '/favicon.ico',
    badge: '/favicon.ico',
    tag: 'unreeled-notify',
    data: { url: data.url || '/' },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(clients.openWindow(event.notification.data.url || '/'));
});
