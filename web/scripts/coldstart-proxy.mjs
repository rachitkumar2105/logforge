// Stands in for a free-tier host in front of the broker's admin API, to check the cold-start
// indicator in a browser without waiting for a real platform. No dependencies.
//
//   node scripts/coldstart-proxy.mjs --port 8090 --target http://127.0.0.1:10000 --cold 30
//
// --cold N   hold every request until N seconds after start, then forward it (a waking service)
// --down     accept connections and drop them at once (a suspended service)
// Point the site at it with LOGFORGE_ADMIN_URL=http://127.0.0.1:8090.
import http from "node:http";

const args = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
};
const port = Number(opt("port", "8090"));
const target = new URL(opt("target", "http://127.0.0.1:10000"));
const coldMs = Number(opt("cold", "0")) * 1000;
const down = args.includes("--down");
const started = Date.now();

const server = http.createServer((req, res) => {
  if (down) {
    req.socket.destroy();
    return;
  }
  const forward = () => {
    const upstream = http.request(
      { host: target.hostname, port: target.port, path: req.url, method: req.method, headers: req.headers },
      (up) => {
        res.writeHead(up.statusCode ?? 502, up.headers);
        up.pipe(res);
      },
    );
    upstream.on("error", () => {
      res.writeHead(502).end();
    });
    req.pipe(upstream);
  };
  const wait = started + coldMs - Date.now();
  if (wait > 0) setTimeout(forward, wait);
  else forward();
});

server.listen(port, "127.0.0.1", () => {
  console.log(`coldstart-proxy on :${port} -> ${target.origin}, ${down ? "down" : `cold for ${coldMs / 1000} s`}`);
});
