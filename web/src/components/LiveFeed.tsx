// Live-колонки на дашборде: награды (очки + стрики) и сырой лог.
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"
import { useLive } from "@/live/LiveProvider"
import { FeedItem } from "./FeedItem"

export function LiveFeed() {
  const { feed } = useLive()
  const rewards = feed.filter((m) => m.body.case !== "logEntry")
  const logs = feed.filter((m) => m.body.case === "logEntry")
  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <FeedCard title="Награды" items={rewards} empty="Пока ничего — ждём баллов и стриков" />
      <FeedCard title="Лог" items={logs} empty="Лог появится здесь в реальном времени" />
    </div>
  )
}

function FeedCard({ title, items, empty }: { title: string; items: ReturnType<typeof useLive>["feed"]; empty: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ScrollArea className="h-80">
          {items.length === 0 ? (
            <p className="text-sm text-muted-foreground">{empty}</p>
          ) : (
            items.map((m) => <FeedItem key={`${m.seq}`} msg={m} />)
          )}
        </ScrollArea>
      </CardContent>
    </Card>
  )
}
