// Live-чат каналов, которые мы смотрим, проксируемый с бэкенда.
// Ничего не сохраняется: строки существуют, только пока открыта эта страница.
import { useLayoutEffect, useRef } from "react"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import type { ChatMessage, ChatModes } from "@/gen/miner/v1/miner_pb"
import { useLive } from "@/live/LiveProvider"

export function ChatPanel() {
  const { state } = useLive()
  const logins = state.slots
    .map((s) => state.channels.find((c) => c.id === s.channelId)?.login)
    .filter((l): l is string => !!l)
  if (logins.length === 0) return null
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {logins.map((login) => (
        <ChatCard key={login} login={login} modes={state.channels.find((c) => c.login === login)?.chatModes} />
      ))}
    </div>
  )
}

/** Понятные подписи для активных ограничений чата (IRC ROOMSTATE). */
function modeLabels(m?: ChatModes): string[] {
  if (!m) return []
  const out: string[] = []
  if (m.followersOnlyMinutes === 0) out.push("только фолловеры")
  if (m.followersOnlyMinutes > 0) out.push(`фолловеры ${m.followersOnlyMinutes} мин`)
  if (m.slowSeconds > 0) out.push(`медленный ${m.slowSeconds} с`)
  if (m.subsOnly) out.push("только сабы")
  if (m.emoteOnly) out.push("только эмоуты")
  if (m.uniqueChat) out.push("уникальные сообщения")
  return out
}

// Короткие подписи для значков, которые стоит показывать в компактном чате.
const BADGES: Record<string, { label: string; className: string }> = {
  broadcaster: { label: "стример", className: "bg-red-600 text-white" },
  moderator: { label: "мод", className: "bg-green-600 text-white" },
  vip: { label: "vip", className: "bg-pink-600 text-white" },
  subscriber: { label: "саб", className: "bg-purple-600 text-white" },
}

function ChatLine({ m }: { m: ChatMessage }) {
  return (
    <div className={`break-words py-0.5 ${m.firstMsg ? "rounded bg-amber-500/15 px-1" : ""}`}>
      {m.badges
        .filter((b) => BADGES[b])
        .map((b) => (
          <span key={b} className={`mr-1 rounded px-1 text-[10px] font-medium ${BADGES[b].className}`}>
            {BADGES[b].label}
          </span>
        ))}
      {m.firstMsg && <span className="mr-1 text-[10px] text-amber-600">новичок</span>}
      <span className="font-semibold" style={{ color: m.color || undefined }}>
        {m.user}
      </span>
      : {m.text}
    </div>
  )
}

const NO_LINES: never[] = []

function ChatCard({ login, modes }: { login: string; modes?: ChatModes }) {
  const { chat } = useLive()
  const lines = chat[login] ?? NO_LINES
  const box = useRef<HTMLDivElement>(null)
  // Прилипать к низу: сразу при открытии экрана и дальше, пока пользователь
  // сам не прокрутил вверх, чтобы почитать. Вернулся вниз — снова прилипаем.
  const stick = useRef(true)
  const onScroll = () => {
    const el = box.current
    if (el) stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40
  }
  useLayoutEffect(() => {
    const el = box.current
    if (el && stick.current) el.scrollTop = el.scrollHeight
  }, [lines])

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          Чат {login}
          {modeLabels(modes).map((l) => (
            <Badge key={l} variant="outline" className="font-normal">
              {l}
            </Badge>
          ))}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div ref={box} onScroll={onScroll} className="h-72 overflow-y-auto pr-2 text-sm">
          {lines.length === 0 && <p className="text-muted-foreground">Ждём сообщений…</p>}
          {lines.map((m, i) => (
            <ChatLine key={i} m={m} />
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
