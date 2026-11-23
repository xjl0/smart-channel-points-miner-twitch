// Два слота просмотра: кого смотрим, почему, как долго и до какого дедлайна.
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { SlotReason, type Slot } from "@/gen/miner/v1/miner_pb"
import { duration, number, reasonLabel, secondsBetween, toDate } from "@/lib/format"
import { useLive, useNow } from "@/live/LiveProvider"

const MAX_SLOTS = 2

export function SlotsPanel() {
  const { state } = useLive()
  const cards = Array.from({ length: MAX_SLOTS }, (_, i) => state.slots[i])
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {cards.map((slot, i) => (slot ? <SlotCard key={slot.channelId} slot={slot} /> : <EmptySlot key={`empty-${i}`} />))}
    </div>
  )
}

function SlotCard({ slot }: { slot: Slot }) {
  const { state } = useLive()
  const now = useNow()
  const ch = state.channels.find((c) => c.id === slot.channelId)
  const started = toDate(slot.startedAt)
  const deadline = toDate(slot.deadline)
  const total = secondsBetween(started, deadline)
  const left = Math.max(0, secondsBetween(now, deadline))
  const progress = total > 0 ? 100 - (left / total) * 100 : 0

  const deadlineLabel =
    slot.reason === SlotReason.STREAK ? (slot.lingering ? "досматриваем" : "до зачёта стрика") : "минимум на канале"

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between gap-2">
          <a className="truncate hover:underline" href={`https://twitch.tv/${ch?.login}`} target="_blank" rel="noreferrer">
            {ch?.displayName ?? slot.channelId}
          </a>
          <Badge variant={slot.reason === SlotReason.STREAK ? "default" : "secondary"}>
            {reasonLabel[slot.reason]}
            {slot.lingering && " ✓"}
          </Badge>
        </CardTitle>
        <CardDescription className="truncate">{ch?.game ? `${ch.game} — ${ch.title}` : ch?.title}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        <div className="grid grid-cols-3 gap-2">
          <Stat label="в слоте" value={duration(secondsBetween(started, now))} />
          <Stat label="за трансляцию" value={duration(ch?.watchedSeconds ?? 0)} />
          <Stat label="зрителей" value={ch?.viewers ? number(ch.viewers) : "—"} />
        </div>
        <div className="space-y-1">
          <div className="flex justify-between text-muted-foreground">
            <span>{deadlineLabel}</span>
            {/* Таймер вышел: решение примет планировщик на ближайшем шаге (он работает раз в 20–40 с). */}
            <span>{left > 0 ? duration(left) : `ждём планировщика (до ${state.settings?.schedulerTickMaxSec ?? 40} с)`}</span>
          </div>
          <Progress value={progress} />
          {slot.beaconFailures > 0 && (
            <p className="text-xs text-red-600">
              минуты просмотра не проходят: {slot.beaconFailures} ошибок подряд — Twitch сейчас не засчитывает
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

function EmptySlot() {
  return (
    <Card className="border-dashed">
      <CardHeader>
        <CardTitle className="text-muted-foreground">Свободный слот</CardTitle>
        <CardDescription>Никого подходящего нет в эфире</CardDescription>
      </CardHeader>
    </Card>
  )
}

export function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-muted-foreground">{label}</div>
      <div className="font-medium">{value}</div>
    </div>
  )
}
