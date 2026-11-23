// Редьюсер состояния на клиенте. Повторяет internal/live/reducer.go: сообщения
// состояния целиком заменяют одну сущность, WatchTick / PointsGained обновляют счётчики.
import type { Account, Channel, LiveMessage, Settings, Slot } from "@/gen/miner/v1/miner_pb"

export interface LiveState {
  account?: Account
  channels: Channel[]
  slots: Slot[]
  queue: string[]
  settings?: Settings
  defaultSettings?: Settings
}

export const emptyState: LiveState = { channels: [], slots: [], queue: [] }

const patchChannel = (s: LiveState, id: string, patch: Partial<Channel>): LiveState => ({
  ...s,
  channels: s.channels.map((c) => (c.id === id ? { ...c, ...patch } : c)),
})

export function apply(s: LiveState, msg: LiveMessage): LiveState {
  const b = msg.body
  switch (b.case) {
    case "snapshot":
      return {
        account: b.value.account,
        channels: b.value.channels,
        slots: b.value.slots,
        queue: b.value.streakQueue,
        settings: b.value.settings,
        defaultSettings: b.value.defaultSettings,
      }
    case "channelUpdated": {
      const ch = b.value.channel!
      const exists = s.channels.some((c) => c.id === ch.id)
      return { ...s, channels: exists ? s.channels.map((c) => (c.id === ch.id ? ch : c)) : [...s.channels, ch] }
    }
    case "channelRemoved":
      return { ...s, channels: s.channels.filter((c) => c.id !== b.value.channelId) }
    case "slotsChanged":
      return { ...s, slots: b.value.slots }
    case "queueChanged":
      return { ...s, queue: b.value.channelIds }
    case "settingsChanged":
      return { ...s, settings: b.value.settings, defaultSettings: b.value.defaults }
    case "accountChanged":
      return { ...s, account: b.value.account }
    case "watchTick":
      return patchChannel(s, b.value.channelId, { watchedSeconds: b.value.watchedSeconds })
    case "pointsGained":
      return patchChannel(s, b.value.channelId, { points: b.value.balance })
    case "viewersChanged": // эфемерное, применяется без seq
      return patchChannel(s, b.value.channelId, { viewers: b.value.viewers })
    default:
      return s // сообщения только для ленты
  }
}

/** Эфемерные сообщения повторяют текущий seq: никогда не сохраняются и не воспроизводятся. */
export const isEphemeral = (msg: LiveMessage) =>
  msg.body.case === "heartbeat" || msg.body.case === "chatMessage" || msg.body.case === "viewersChanged"

/** Дельты увеличивают seq на единицу; Snapshot и эфемерные сообщения лишь повторяют его. */
export const isDelta = (msg: LiveMessage) => msg.body.case !== "snapshot" && !isEphemeral(msg)
