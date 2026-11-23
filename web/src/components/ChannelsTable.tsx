// Отслеживаемые каналы: добавление, пауза/возобновление, удаление; статус эфира и счётчики.
import { useState, type FormEvent } from "react"

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import type { Channel } from "@/gen/miner/v1/miner_pb"
import { duration, expiresIn, number, toDate } from "@/lib/format"
import { useCommand } from "@/lib/query"
import { rpc } from "@/lib/rpc"
import { useLive } from "@/live/LiveProvider"

export function ChannelsTable() {
  const { state } = useLive()
  const channels = [...state.channels].sort(
    (a, b) => Number(b.online) - Number(a.online) || a.login.localeCompare(b.login),
  )
  return (
    <Card>
      <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-3">
        <CardTitle>Каналы ({channels.length})</CardTitle>
        <AddChannelForm />
      </CardHeader>
      <CardContent className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Канал</TableHead>
              <TableHead>Статус</TableHead>
              <TableHead>Стрик</TableHead>
              <TableHead className="text-right">Смотрели</TableHead>
              <TableHead className="text-right">Баллы</TableHead>
              <TableHead>Вкл.</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {channels.map((ch) => (
              <ChannelRow key={ch.id} ch={ch} />
            ))}
            {channels.length === 0 && (
              <TableRow>
                <TableCell colSpan={7} className="text-center text-muted-foreground">
                  Добавьте первый канал
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

function ChannelRow({ ch }: { ch: Channel }) {
  const setEnabled = useCommand("Переключение канала", (enabled: boolean) =>
    rpc.setChannelEnabled({ channelId: ch.id, enabled }),
  )
  return (
    <TableRow className={ch.enabled ? "" : "opacity-50"}>
      <TableCell>
        <a className="font-medium hover:underline" href={`https://twitch.tv/${ch.login}`} target="_blank" rel="noreferrer">
          {ch.displayName}
        </a>
        {ch.online && <div className="max-w-64 truncate text-xs text-muted-foreground">{ch.game}</div>}
      </TableCell>
      <TableCell>
        <StatusBadge ch={ch} />
      </TableCell>
      <TableCell>
        <StreakBadge ch={ch} />
        <div className="text-xs text-muted-foreground">
          длина {ch.streakLength}
          {ch.streakExpiresAt && ` · сгорит через ${expiresIn(toDate(ch.streakExpiresAt))}`}
          {ch.streakBonus.length > 0 && ` · бонус ${streakBonusLabel(ch.streakBonus)}`}
        </div>
        <RetryStreakButton ch={ch} />
      </TableCell>
      <TableCell className="text-right tabular-nums">{ch.online ? duration(ch.watchedSeconds) : "—"}</TableCell>
      <TableCell className="text-right tabular-nums">
        {number(ch.points)}
        {ch.pointsBonus > 0 && (
          <span className="ml-1 text-xs text-muted-foreground" title="Активные множители баллов">
            +{Math.round(ch.pointsBonus * 100)}%
          </span>
        )}
      </TableCell>
      <TableCell>
        <Switch
          checked={ch.enabled}
          onCheckedChange={(enabled) =>
            setEnabled.mutate(enabled)
          }
        />
      </TableCell>
      <TableCell>
        <RemoveButton ch={ch} />
      </TableCell>
    </TableRow>
  )
}

function StatusBadge({ ch }: { ch: Channel }) {
  if (ch.missing) return <Badge variant="destructive" title="Бан или смена логина">не найден</Badge>
  if (!ch.online) return <Badge variant="outline">оффлайн</Badge>
  return (
    <span className="whitespace-nowrap">
      <Badge>в эфире</Badge>
      {ch.viewers > 0 && <span className="ml-1 text-xs text-muted-foreground">👁 {number(ch.viewers)}</span>}
    </span>
  )
}

function StreakBadge({ ch }: { ch: Channel }) {
  if (!ch.online) return <span className="text-muted-foreground">—</span>
  if (ch.streakClaimed) return <Badge>получен</Badge>
  if (ch.streakRetry) return <Badge variant="outline">повтор, первый в очереди</Badge>
  return <Badge variant="secondary">ждёт</Badge>
}

/** "300" или "300–450": очки, которые канал даёт за стрик (по уровню). */
const streakBonusLabel = (tiers: number[]) =>
  tiers.length === 1 ? `${tiers[0]}` : `${Math.min(...tiers)}–${Math.max(...tiers)}`

/** Повторить стрик, засчитанный (или считающийся полученным) на этой трансляции. */
function RetryStreakButton({ ch }: { ch: Channel }) {
  const retry = useCommand("Повтор стрика", () => rpc.retryStreak({ channelId: ch.id }))
  if (!ch.online || !ch.streakClaimed) return null
  return (
    <Button
      variant="link"
      size="sm"
      className="h-auto p-0 text-xs"
      title="Забыть стрик этой трансляции и поставить канал первым в очередь"
      disabled={retry.isPending}
      onClick={() => retry.mutate(undefined)}
    >
      Повторить стрик
    </Button>
  )
}

function AddChannelForm() {
  const [login, setLogin] = useState("")
  const add = useCommand(
    "Добавление канала",
    (l: string) => rpc.addChannel({ login: l }),
    (r) => `Канал ${r.channel?.login} добавлен`,
  )
  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!login.trim()) return
    add.mutate(login, { onSuccess: () => setLogin("") })
  }
  return (
    <form onSubmit={submit} className="flex gap-2">
      <Input placeholder="логин канала" value={login} onChange={(e) => setLogin(e.target.value)} className="w-48" />
      <Button type="submit" disabled={add.isPending}>
        Добавить
      </Button>
    </form>
  )
}

function RemoveButton({ ch }: { ch: Channel }) {
  const remove = useCommand("Удаление канала", () => rpc.removeChannel({ channelId: ch.id }))
  return (
    <AlertDialog>
      <AlertDialogTrigger asChild>
        <Button variant="ghost" size="sm">
          Удалить
        </Button>
      </AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Удалить {ch.displayName}?</AlertDialogTitle>
          <AlertDialogDescription>
            История просмотров и трансляций канала будет удалена. Чтобы просто приостановить — выключите переключатель.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Отмена</AlertDialogCancel>
          <AlertDialogAction onClick={() => remove.mutate(undefined)}>
            Удалить
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
