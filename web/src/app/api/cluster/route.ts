import { getCluster } from "@/lib/cluster";

// GET only: the public dashboard reads through here. Any other method gets 405 from Next.js.
export const dynamic = "force-dynamic";

export async function GET() {
  return Response.json(await getCluster(), {
    headers: { "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff" },
  });
}
