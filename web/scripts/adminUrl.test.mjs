// SECURITY-AUDIT F6: the read token must never travel to the broker over plain HTTP.
import assert from "node:assert/strict";
import { test } from "node:test";
import { safeAdminUrl } from "../src/lib/adminUrl.ts";

test("https admin URLs are used as given", () => {
  assert.equal(safeAdminUrl("https://logforge-broker.onrender.com")?.origin, "https://logforge-broker.onrender.com");
});

test("plain http is allowed only to this machine", () => {
  assert.equal(safeAdminUrl("http://127.0.0.1:8080")?.port, "8080");
  assert.equal(safeAdminUrl("http://localhost:8080")?.hostname, "localhost");
  assert.equal(safeAdminUrl("http://[::1]:8080")?.hostname, "[::1]");
  assert.equal(safeAdminUrl("http://logforge-broker.onrender.com"), null);
  assert.equal(safeAdminUrl("http://10.0.0.5:8080"), null);
});

test("anything else is refused", () => {
  for (const bad of [undefined, "", "not a url", "ftp://example.com", "javascript:alert(1)"]) {
    assert.equal(safeAdminUrl(bad), null, String(bad));
  }
});
