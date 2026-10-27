// The broker's admin URL from server configuration, checked before any token is sent to it.
// No imports, so scripts/adminUrl.test.mjs can load this file directly with Node.

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

/**
 * The admin API base URL, or null if it must not be used: anything but https is refused unless it
 * points at this machine, so the read token never crosses a network in clear text (SECURITY-AUDIT
 * F6). TLS for the hosted broker ends at the platform's edge.
 */
export function safeAdminUrl(raw: string | undefined): URL | null {
  if (!raw) return null;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (url.protocol === "https:") return url;
  if (url.protocol === "http:" && LOCAL_HOSTS.has(url.hostname)) return url;
  return null;
}
