// Страница «Настройки»: все таймеры движка с объяснениями. Сохранение
// применяется сразу (без перезапуска) и приходит во все открытые админки
// через live-поток. Границы и связи значений проверяет бэкенд.
import { useEffect, useState } from "react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import type { Settings } from "@/gen/miner/v1/miner_pb"
import { useCommand } from "@/lib/query"
import { rpc } from "@/lib/rpc"
import { useLive } from "@/live/LiveProvider"

/** Числовые поля сообщения Settings (все — в секундах). */
type Key = Exclude<keyof Settings, "$typeName" | "$unknown">
type Values = Record<Key, number>
type Unit = "мин" | "с"

/** Одно значение или диапазон «от–до» (случайный выбор внутри). */
interface Field {
  keys: [Key] | [Key, Key]
  label: string
  unit: Unit
  help: string
}

interface Group {
  title: string
  description: string
  fields: Field[]
}

const GROUPS: Group[] = [
  {
    title: "Стрики",
    description: "Как программа добивается стрика на канале, который вышел в эфир.",
    fields: [
      {
        keys: ["streakAssumeAfterSec"],
        label: "Считать стрик полученным после",
        unit: "мин",
        help:
          "Если за столько засчитанных минут просмотра трансляции стрик не пришёл, программа считает, что он уже был получен раньше (например, с телефона), и отпускает канал. Минуты, когда PubSub был отключён, не считаются. Обычно Twitch выдаёт стрик за 5–10 минут.",
      },
      {
        keys: ["streakAttemptMaxSec"],
        label: "Потолок попытки стрика",
        unit: "мин",
        help:
          "Жёсткий предел одной попытки по часам. Если минуты не засчитываются (обрыв сети, PubSub, блокировка), по истечении этого времени канал отпускается без зачёта стрика, чтобы не держать слот вечно. Не меньше значения выше.",
      },
      {
        keys: ["streakCooldownSec"],
        label: "Отдых канала после потолка",
        unit: "мин",
        help: "Сколько канал, упёршийся в потолок попытки, пропускает других, прежде чем снова встать в очередь стриков.",
      },
      {
        keys: ["streakLingerSec"],
        label: "Досмотр после стрика",
        unit: "мин",
        help:
          "Сколько ещё смотреть канал после получения стрика, чтобы не уходить в ту же секунду. Фактическое время случайное: ±1/3 от значения.",
      },
      {
        keys: ["streakCheckEverySec"],
        label: "Проверять стрик раз в",
        unit: "мин",
        help:
          "Как часто во время попытки спрашивать у Twitch (RewardList), не пришёл ли стрик. Стрик с бонусом приходит событием сразу; проверка — страховка для стриков без события.",
      },
    ],
  },
  {
    title: "Обычный просмотр",
    description: "Когда все стрики собраны, программа смотрит каналы, которые меньше всех смотрели за их трансляцию.",
    fields: [
      {
        keys: ["fillHoldSec"],
        label: "Минимум на канале",
        unit: "мин",
        help:
          "Сколько держать канал, прежде чем можно переключиться на менее просмотренный, чтобы не прыгать между каналами. Фактическое время случайное: ±1/3. Канал с несобранным стриком может прервать раньше.",
      },
    ],
  },
  {
    title: "Сундуки и моменты",
    description: "Только на каналах в слотах программы — сундуки каналов, которые вы смотрите сами, остаются вам.",
    fields: [
      {
        keys: ["chestDelayMinSec", "chestDelayMaxSec"],
        label: "Пауза перед кликом сундука",
        unit: "с",
        help: "«Время реакции»: выбирается случайно в этом диапазоне. Если сундук за это время заберёт другой клиент аккаунта, клика не будет.",
      },
      {
        keys: ["momentDelayMinSec", "momentDelayMaxSec"],
        label: "Пауза перед получением момента",
        unit: "с",
        help: "То же для «моментов» сообщества.",
      },
    ],
  },
  {
    title: "Проверки",
    description: "Фоновые циклы. Интервалы случайные внутри диапазона, чтобы запросы не шли по ровному расписанию.",
    fields: [
      {
        keys: ["onlinePollMinSec", "onlinePollMaxSec"],
        label: "Проверка онлайна всех каналов",
        unit: "мин",
        help:
          "Страховка к событиям PubSub: все каналы проверяются одним пакетным запросом. После переподключения PubSub или долгой паузы проверка запускается сразу, не дожидаясь интервала.",
      },
      {
        keys: ["schedulerTickMinSec", "schedulerTickMaxSec"],
        label: "Шаг планировщика",
        unit: "с",
        help:
          "Как часто программа пересматривает, кого смотреть. Поэтому после конца таймера слота бывает «ждём планировщика» — не дольше верхней границы.",
      },
    ],
  },
]

const KEYS = GROUPS.flatMap((g) => g.fields.flatMap((f) => f.keys))

const toValues = (s: Settings): Values => Object.fromEntries(KEYS.map((k) => [k, s[k]])) as Values
const toUnit = (sec: number, unit: Unit) => (unit === "мин" ? sec / 60 : sec)
const fromUnit = (v: number, unit: Unit) => Math.round(unit === "мин" ? v * 60 : v)
const fmt = (sec: number, unit: Unit) => `${+toUnit(sec, unit).toFixed(2)} ${unit}`
const same = (a: Values, b: Values) => KEYS.every((k) => a[k] === b[k])

export function SettingsPage() {
  const { state } = useLive()
  const current = state.settings
  const defaults = state.defaultSettings
  const [draft, setDraft] = useState<Values | null>(null)
  const saveCmd = useCommand(
    "Сохранение настроек",
    (settings: Values) => rpc.updateSettings({ settings }),
    "Настройки применены",
  )

  // Пока пользователь ничего не менял, черновик следует за живыми настройками
  // (в том числе если их поменяли в другой вкладке).
  const dirty = draft !== null && current !== undefined && !same(draft, toValues(current))
  useEffect(() => {
    if (current && !dirty) setDraft(toValues(current))
  }, [current]) // eslint-disable-line react-hooks/exhaustive-deps

  if (!current || !defaults || !draft) {
    return <p className="text-sm text-muted-foreground">Загрузка настроек…</p>
  }

  const save = () => saveCmd.mutate(draft)

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">Настройки</h2>
          <p className="text-sm text-muted-foreground">
            Применяются сразу после сохранения, без перезапуска: текущие слоты пересчитываются под новые значения.
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => setDraft(toValues(defaults))} disabled={same(draft, toValues(defaults))}>
            Вернуть стандартные
          </Button>
          <Button variant="outline" onClick={() => setDraft(toValues(current))} disabled={!dirty}>
            Отменить
          </Button>
          <Button onClick={save} disabled={!dirty || saveCmd.isPending}>
            {saveCmd.isPending ? "Сохранение…" : "Сохранить"}
          </Button>
        </div>
      </div>

      {GROUPS.map((g) => (
        <Card key={g.title}>
          <CardHeader>
            <CardTitle>{g.title}</CardTitle>
            <CardDescription>{g.description}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-5">
            {g.fields.map((f) => (
              <FieldRow
                key={f.keys[0]}
                field={f}
                values={draft}
                defaults={toValues(defaults)}
                saved={toValues(current)}
                onChange={(k, sec) => setDraft({ ...draft, [k]: sec })}
              />
            ))}
          </CardContent>
        </Card>
      ))}

      <Card>
        <CardHeader>
          <CardTitle>Неизменяемые правила</CardTitle>
          <CardDescription>Заложены в программу и здесь не настраиваются.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <p>
            <b>Не больше 2 каналов и 2 чатов одновременно.</b> Жёсткое правило: так аккаунт выглядит как обычный зритель.
          </p>
          <p>
            <b>Минута просмотра (minute-watched) раз в ~60 с (±4 с).</b> Так считает просмотр сам Twitch, иначе время не
            засчитывается.
          </p>
          <p>
            <b>Разброс ±1/3</b> для «досмотра после стрика» и «минимума на канале» — чтобы переключения не шли по ровному
            расписанию.
          </p>
        </CardContent>
      </Card>
    </div>
  )
}

function FieldRow(props: {
  field: Field
  values: Values
  defaults: Values
  saved: Values
  onChange: (k: Key, sec: number) => void
}) {
  const { field, values, defaults, saved, onChange } = props
  const changed = field.keys.some((k) => values[k] !== saved[k])
  const def = field.keys.map((k) => fmt(defaults[k], field.unit)).join(" – ")
  return (
    <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_auto] md:gap-6">
      <div className="space-y-1">
        <div className="font-medium">
          {field.label}
          {changed && <span className="ml-2 text-xs font-normal text-amber-600">изменено, не сохранено</span>}
        </div>
        <p className="text-sm text-muted-foreground">{field.help}</p>
        <p className="text-xs text-muted-foreground">По умолчанию: {def}</p>
      </div>
      <div className="flex items-center gap-2 md:pt-1">
        {field.keys.map((k, i) => (
          <span key={k} className="flex items-center gap-2">
            {field.keys.length === 2 && <span className="text-sm text-muted-foreground">{i === 0 ? "от" : "до"}</span>}
            <Input
              type="number"
              min={0}
              step={field.unit === "мин" ? 0.5 : 1}
              className="w-24"
              value={toUnit(values[k], field.unit)}
              onChange={(e) => {
                const v = Number(e.target.value)
                if (Number.isFinite(v) && v >= 0) onChange(k, fromUnit(v, field.unit))
              }}
            />
          </span>
        ))}
        <span className="text-sm text-muted-foreground">{field.unit}</span>
      </div>
    </div>
  )
}
