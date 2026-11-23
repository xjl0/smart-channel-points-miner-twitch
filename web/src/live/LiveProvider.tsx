// Держит live-поток открытым и отдаёт приложению свёрнутое состояние.
//
// Транспорт: обычный поток Server-Sent Events (GET /live), читаемый через
// EventSource браузера. Каждое событие — один LiveMessage в JSON. EventSource
// переподключается сам и отправляет обратно id последнего события (= seq), так что
// сервер воспроизводит ровно то, что мы пропустили. Мы вмешиваемся только при пропуске
// seq или когда перестают приходить heartbeat'ы: тогда переоткрываем с ?since=<last seq>.
import { createContext, useContext, useEffect, useState, type ReactNode } from "react"

import { fromJson } from "@bufbuild/protobuf"

import { LiveMessageSchema, type ChatMessage, type LiveMessage } from "@/gen/miner/v1/miner_pb"
import { apply, emptyState, isDelta, isEphemeral, type LiveState } from "./reducer"

export type ConnStatus = "connecting" | "live" | "offline"

interface LiveContextValue {
  state: LiveState
  status: ConnStatus
  /** Последние сообщения ленты (очки, стрики, логи), сначала новые. */
  feed: LiveMessage[]
  /** Проксированные строки чата по логину канала, сначала старые (только в памяти). */
  chat: Record<string, ChatMessage[]>
  lastSeq: bigint
}

const FEED_LIMIT = 300
const HEARTBEAT_TIMEOUT_MS = 40_000 // сервер шлёт его каждые 15 с
const FEED_CASES = new Set(["pointsGained", "streakResult", "logEntry"])
const CHAT_LIMIT = 300 // строк хранится на канал
const CHAT_FLUSH_MS = 500 // быстрые чаты отрисовываются пачками

const LiveContext = createContext<LiveContextValue | null>(null)

class SeqGap extends Error {}

// Входящие строки чата, ожидающие следующего сброса (на уровне модуля: один поток на страницу).
const chatBuffer: ChatMessage[] = []

export function LiveProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<LiveState>(emptyState)
  const [status, setStatus] = useState<ConnStatus>("connecting")
  const [feed, setFeed] = useState<LiveMessage[]>([])
  const [lastSeq, setLastSeq] = useState(0n)
  const [chat, setChat] = useState<Record<string, ChatMessage[]>>({})

  // Строки чата буферизуются и сбрасываются дважды в секунду, чтобы не
  // перерисовывать на каждое сообщение оживлённого чата.
  useEffect(() => {
    const flush = setInterval(() => {
      if (chatBuffer.length === 0) return
      const batch = chatBuffer.splice(0)
      setChat((prev) => {
        const next = { ...prev }
        for (const m of batch) {
          next[m.channelLogin] = [...(next[m.channelLogin] ?? []), m].slice(-CHAT_LIMIT)
        }
        return next
      })
    }, CHAT_FLUSH_MS)
    return () => clearInterval(flush)
  }, [])

  useEffect(() => {
    let es: EventSource | null = null
    let seq = 0n
    let lastMessageAt = Date.now()
    let reopenTimer: ReturnType<typeof setTimeout> | undefined

    const handle = (msg: LiveMessage) => {
      lastMessageAt = Date.now()
      if (isEphemeral(msg)) {
        if (msg.seq > seq) throw new SeqGap() // мы что-то молча пропустили
        if (msg.body.case === "chatMessage") chatBuffer.push(msg.body.value)
        if (msg.body.case === "viewersChanged") setState((s) => apply(s, msg))
        return
      }
      if (isDelta(msg) && seq !== 0n && msg.seq !== seq + 1n) throw new SeqGap()
      seq = msg.seq
      setLastSeq(seq)
      setState((s) => apply(s, msg))
      if (FEED_CASES.has(msg.body.case ?? "")) setFeed((f) => [msg, ...f].slice(0, FEED_LIMIT))
    }

    // (Пере)открыть поток с нашего последнего применённого seq.
    const open = () => {
      clearTimeout(reopenTimer)
      es?.close()
      setStatus("connecting")
      lastMessageAt = Date.now()
      es = new EventSource(`/live?since=${seq}`)
      es.onmessage = (e) => {
        try {
          handle(fromJson(LiveMessageSchema, JSON.parse(e.data)))
          setStatus("live")
        } catch (err) {
          if (!(err instanceof SeqGap)) console.warn("bad live event", err)
          open() // продолжить с последнего корректного seq
        }
      }
      es.onerror = () => {
        setStatus("offline")
        // EventSource повторяет попытки сам, если только сервер не отказал в потоке.
        if (es?.readyState === EventSource.CLOSED) reopenTimer = setTimeout(open, 5_000)
      }
    }
    open()

    // Сторожевой таймер: слишком долго нет сообщений (даже heartbeat) → переоткрыть.
    const watchdog = setInterval(() => {
      if (Date.now() - lastMessageAt > HEARTBEAT_TIMEOUT_MS) open()
    }, 5_000)

    return () => {
      clearInterval(watchdog)
      clearTimeout(reopenTimer)
      es?.close()
    }
  }, [])

  return <LiveContext.Provider value={{ state, status, feed, chat, lastSeq }}>{children}</LiveContext.Provider>
}

export function useLive(): LiveContextValue {
  const ctx = useContext(LiveContext)
  if (!ctx) throw new Error("useLive must be used inside <LiveProvider>")
  return ctx
}

/** Перерисовывает каждые `ms`, чтобы обратные отсчёты и таймеры оставались актуальными. */
export function useNow(ms = 1_000): Date {
  const [now, setNow] = useState(() => new Date())
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), ms)
    return () => clearInterval(t)
  }, [ms])
  return now
}
