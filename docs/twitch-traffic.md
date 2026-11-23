# Что присылает Twitch: разбор трассировки

Источник: `trace/2026-11-22.jsonl` (≈22 мин, 21:57–22:19 UTC, 30 каналов, 2 слота). Сводку строит
`python3 scripts/trace_report.py trace [--channel <id>]`. Формат файла: одна JSON-строка на событие,
поля `ts`, `src` (`gql` | `pubsub` | `irc` | `spade`), `ev` (`in` | `out` | `exchange` | `minute-watched`).

## Главные выводы

1. **Без живого PubSub Twitch не засчитывает просмотр.** 21:57–22:16 PubSub падал сразу после
   подключения (`close 1009 message too big` — все темы в одном LISTEN). Spade при этом отвечал 204 на
   каждую «минуту», но баланс канала не изменился (4230 → 4230), и стрика не было.
   Через 11 с после восстановления PubSub (22:16:52) — `achievementTimestamp` 22:17:03, через 14 с — WATCH +10.
   ⇒ PubSub-подключение с темой `community-points-user-v1` — обязательная часть «присутствия».
   Его падение теперь логируется как ERROR.
2. **Стрик обычной длины не приходит в PubSub.** `points-earned` с `WATCH_STREAK` бывает только когда за стрик
   начисляется бонус: `watchStreakPoints` = 300/350/400/450 (по уровням, `watchStreakThreshold: 2`).
   Надёжный источник — `RewardList` (`achievementTimestamp` > начала трансляции).
3. **Время начала трансляции есть только в `WithIsStreamLiveQuery`** (`stream.createdAt`);
   `VideoPlayerStreamInfoOverlayChannel` его не отдаёт.
4. Spade: 47/47 ответов 204. GQL: 35 запросов, 0 ошибок, 184–365 мс (медиана 245).

## GQL (`src: gql`, `ev: exchange` — запрос и ответ целиком)

| Операция | Когда | Что полезного в ответе |
|---|---|---|
| `VideoPlayerStreamInfoOverlayChannel` + `WithIsStreamLiveQuery` | пакетом по 10 каналов, старт и раз в 10–15 мин | id трансляции, зрители, название, игра, `displayName`; `createdAt` — только во втором |
| `RewardList` | выход в эфир, затем раз в 3 мин во время попытки | `watchStreakMilestone`: `state` (`NONE`/`ACTIVE`), `value` (длина), `achievementTimestamp`, `watchStreakThreshold`, `watchStreakCopoBonus`, `missedStreams`, `expiresAt` (пока всегда `null`) |
| `ChannelPointsContext` | старт слота | баланс, сундук (`availableClaim`), множители; `earning`: `passiveWatchPoints` 10 / 5 мин, `claimPoints` 50, `averagePointsPerHour` ≈220, `watchStreakPoints` 300–450, `followPoints`, `raidPoints` |
| `GetIDFromLogin` | добавление канала | id; `null` у забаненного |

## PubSub (`src: pubsub`)

- `out`: `LISTEN`/`UNLISTEN` (без токена), `PING` раз в 2–4 мин. `in`: `RESPONSE` на каждый LISTEN
  (132 шт., все без ошибок), `PONG`, `MESSAGE`.
- `video-playback-by-id` → `viewcount` каждые ~30 с по каждому живому каналу: `viewers`, `server_time`,
  `collaboration_*`. Это живой счётчик зрителей без опроса; `stream-up`/`stream-down` — начало/конец.
- `community-points-user-v1` → `points-earned`: `point_gain {total_points, baseline_points, reason_code, multipliers}`,
  `balance`; `claim-available` — сундук.
- За 22 минуты не пришло ничего по `onsite-notifications`, `user-drop-events`, `predictions-*`,
  `raid`, `hype-train-events-v1`, `broadcast-settings-update`, `community-moments-channel-v1`
  (события редкие — копим дальше).

## IRC / чат (`src: irc`)

- Служебное при входе: `CAP ACK`, `001–004/375/372/376` (приветствие), `GLOBALUSERSTATE`,
  на каждый канал `JOIN`, `353/366` (список имён — Twitch показывает только нас), `ROOMSTATE`
  (режимы чата: `emote-only`, `followers-only`, `slow`, `subs-only`, `r9k`), `USERSTATE`
  (наши бейджи/цвет/наборы эмоутов в этом канале), `PING` от сервера ≈ раз в 5 мин.
- `PRIVMSG` (150 за 22 мин на двух каналах) с тегами: `display-name`, `color`, `badges`, `emotes`,
  `first-msg`, `returning-chatter`, `mod`, `vip`, `subscriber`, `tmi-sent-ts`, `user-id`, `id`.

## Что из этого можно взять в работу

- Зрители онлайн по каждому каналу из `viewcount` — бесплатно, без запросов.
- Бейджи (mod/vip/sub/first-msg) в окне чата из тегов `PRIVMSG`.
- «Стрик даёт N баллов» в админке из `earning.watchStreakPoints`.
- Режимы чата из `ROOMSTATE` (например, followers-only).
