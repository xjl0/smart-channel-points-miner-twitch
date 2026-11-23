// Одна строка ленты событий / истории (очки, результаты стриков, записи лога).
import { Badge } from "@/components/ui/badge"
import type { LiveMessage } from "@/gen/miner/v1/miner_pb"
import { clock, dateTime, number, reasonLabel, toDate } from "@/lib/format"
import { useLive } from "@/live/LiveProvider"

const levelVariant: Record<string, "default" | "secondary" | "destructive" | "outline"> = {
  ERROR: "destructive",
  WARN: "outline",
  INFO: "secondary",
}

export function FeedItem({ msg, fullDate = false }: { msg: LiveMessage; fullDate?: boolean }) {
  const { state } = useLive()
  const name = (id: string) => state.channels.find((c) => c.id === id)?.displayName ?? id
  const ts = toDate(msg.ts)
  const when = fullDate ? dateTime(ts) : clock(ts)

  let badge: React.ReactNode
  let text: React.ReactNode
  const b = msg.body
  switch (b.case) {
    case "pointsGained":
      badge = <Badge>+{number(b.value.points)}</Badge>
      text = `${name(b.value.channelId)}: ${b.value.reason}, баланс ${number(b.value.balance)}`
      break
    case "streakResult":
      badge = b.value.assumed ? <Badge variant="secondary">стрик ≈</Badge> : <Badge>стрик ✓</Badge>
      text = b.value.assumed
        ? `${name(b.value.channelId)}: не пришёл за 15 мин просмотра — считаем полученным ранее`
        : `${name(b.value.channelId)}: стрик получен (${b.value.streakLength})`
      break
    case "slotsChanged":
      badge = <Badge variant="outline">слоты</Badge>
      text = b.value.slots.map((s) => `${name(s.channelId)} (${reasonLabel[s.reason]})`).join(", ") || "никого не смотрим"
      break
    case "channelRemoved":
      badge = <Badge variant="outline">канал</Badge>
      text = `удалён ${b.value.channelId}`
      break
    case "logEntry": {
      const attrs = Object.entries(b.value.attrs)
        .filter(([k]) => k !== "component" && k !== "channel_id")
        .map(([k, v]) => `${k}=${v}`)
        .join(" ")
      badge = <Badge variant={levelVariant[b.value.level] ?? "secondary"}>{b.value.level}</Badge>
      text = (
        <>
          {b.value.message} <span className="text-muted-foreground">{attrs}</span>
        </>
      )
      break
    }
    default:
      return null
  }

  return (
    <div className="flex items-start gap-2 py-1 text-sm">
      <span className="shrink-0 tabular-nums text-muted-foreground">{when}</span>
      <span className="shrink-0">{badge}</span>
      <span className="min-w-0 break-words">{text}</span>
    </div>
  )
}
