import { checkHealth } from "@/lib/cluster";

// The cold-start indicator's ping. The browser never learns the broker's address or token.
export const dynamic = "force-dynamic";
export const maxDuration = 10;

export async function GET() {
  return Response.json(await checkHealth(), {
    headers: { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff" },
  });
}
