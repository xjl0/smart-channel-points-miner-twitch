// Каналы, ожидающие попытки стрика, в том порядке, в котором они будут взяты.
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { expiresIn, toDate } from "@/lib/format"
import { useLive } from "@/live/LiveProvider"

export function QueueCard() {
  const { state } = useLive()
  const channel = (id: string) => state.channels.find((c) => c.id === id)
  return (
    <Card>
      <CardHeader>
        <CardTitle>Очередь стриков</CardTitle>
      </CardHeader>
      <CardContent>
        {state.queue.length === 0 ? (
          <p className="text-sm text-muted-foreground">Пусто — все стрики в работе или получены</p>
        ) : (
          <ol className="list-decimal space-y-1 pl-5 text-sm">
            {state.queue.map((id) => {
              const ch = channel(id)
              const exp = expiresIn(toDate(ch?.streakExpiresAt))
              return (
                <li key={id}>
                  {ch?.displayName ?? id}
                  {ch?.streakRetry && <span className="text-muted-foreground"> (повтор вручную)</span>}
                  {exp && <span className="text-muted-foreground"> — стрик {ch?.streakLength}, сгорит через {exp}</span>}
                </li>
              )
            })}
          </ol>
        )}
      </CardContent>
    </Card>
  )
}
