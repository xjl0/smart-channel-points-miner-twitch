// Небольшие вспомогательные функции отображения, общие для компонентов.
import { timestampDate, type Timestamp } from "@bufbuild/protobuf/wkt"

import { SlotReason } from "@/gen/miner/v1/miner_pb"

export const toDate = (ts?: Timestamp) => (ts ? timestampDate(ts) : undefined)

/** 3725s → "1 ч 2 мин", 59s → "59 с". */
export function duration(seconds: number | bigint): string {
  const s = Math.max(0, Math.floor(Number(seconds)))
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  if (h > 0) return `${h} ч ${m} мин`
  if (m > 0) return `${m} мин`
  return `${s % 60} с`
}

/** Секунды от a до b (по умолчанию b — текущий момент). */
export const secondsBetween = (a?: Date, b: Date = new Date()) => (a ? (b.getTime() - a.getTime()) / 1000 : 0)

export const clock = (d?: Date) => (d ? d.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "")

export const dateTime = (d?: Date) => (d ? d.toLocaleString("ru-RU", { dateStyle: "short", timeStyle: "medium" }) : "")

export const reasonLabel: Record<SlotReason, string> = {
  [SlotReason.UNSPECIFIED]: "—",
  [SlotReason.STREAK]: "стрик",
  [SlotReason.FILL]: "просмотр",
}

export const number = (n: number | bigint) => Number(n).toLocaleString("ru-RU")

/** Обратный отсчёт до будущего момента в стиле "через 1 д 3 ч" ("", если неизвестен или уже прошёл). */
export function expiresIn(d?: Date, now: Date = new Date()): string {
  if (!d) return ""
  const s = (d.getTime() - now.getTime()) / 1000
  if (s <= 0) return "сгорел"
  const days = Math.floor(s / 86400)
  return days > 0 ? `${days} д ${Math.floor((s % 86400) / 3600)} ч` : duration(s)
}
