#!/usr/bin/env python3
"""Вырезать реальные payload'ы Twitch из трассы трафика в тестовые фикстуры.

Запуск: python3 scripts/extract_fixtures.py [trace_dir] [out_dir]
       (по умолчанию: trace, internal/engine/testdata/twitch)

Фейковый Twitch в тестах движка отдаёт эти payload'ы, подменяя только
поля, которыми управляет сценарий (логин, live/offline, стрик, баланс), так что
тесты работают с точно такими структурами, какие шлёт Twitch. Наш собственный user id
заменяется на 1000; секретов в трассе изначально нет.
"""
import glob
import json
import os
import re
import sys

trace_dir = sys.argv[1] if len(sys.argv) > 1 else "trace"
out_dir = sys.argv[2] if len(sys.argv) > 2 else "internal/engine/testdata/twitch"
os.makedirs(out_dir, exist_ok=True)

# ---------- обезличивание ----------
# Фикстуры лежат в публичном репозитории: всё, что указывает на конкретный
# канал или зрителя, заменяется условным. Тестам важна форма ответов Twitch,
# а не значения.
FAKE_IDS = {}


def fake_id(real):
    """Стабильная замена числового id канала/трансляции: 10000001, 10000002…"""
    return FAKE_IDS.setdefault(real, str(10000001 + len(FAKE_IDS)))


ID_KEYS = {"id", "channel_id", "channelID", "room_id", "broadcast_id"}


def anonymize(v, path=()):
    if isinstance(v, dict):
        return {k: anonymize(x, path + (k,)) for k, x in v.items()}
    if isinstance(v, list):
        return [anonymize(x, path) for x in v]
    if not isinstance(v, str):
        return v
    key = path[-1] if path else ""
    in_game = "game" in path
    if v.startswith("https://static-cdn.jtvnw.net/"):
        return "https://static-cdn.jtvnw.net/example.png"
    if key in ("login",):
        return "channel_a"
    if key == "displayName" and not in_game:
        return "Channel A"
    if key == "profileURL":
        return "https://www.twitch.tv/channel_a"
    if key == "title":
        return "Название"
    if key == "prompt":
        return "Описание награды"
    if key == "token":  # коды эмоутов канала содержат его префикс
        return "chanEmote"
    if key in ID_KEYS and v.isdigit() and len(v) >= 6:
        return fake_id(v)
    if key == "id" and ":" in v and v.split(":")[0].isdigit():  # "<channel id>:SEND_ANIMATED_MESSAGE"
        head, tail = v.split(":", 1)
        return fake_id(head) + ":" + tail
    return v


def anonymize_topic(topic):
    """Топик PubSub: id канала в конце ("video-playback-by-id.<id>")."""
    return re.sub(r"\.(\d{6,12})$", lambda m: "." + fake_id(m.group(1)), topic)


def anonymize_text(s):
    """Строки IRC и итоговый текст JSON: room-id, имя канала в командах IRC и
    уже известные id каналов — одним проходом, без цепочек замен."""
    s = re.sub(r"(room-id=)(\d+)", lambda m: m.group(1) + fake_id(m.group(2)), s)
    s = re.sub(r"(ROOMSTATE|PRIVMSG|JOIN|PART) #\w+", r"\1 #channel_a", s)
    known = [k for k in FAKE_IDS if k not in FAKE_IDS.values()]
    if known:
        s = re.sub(r"\b(" + "|".join(map(re.escape, known)) + r")\b", lambda m: FAKE_IDS[m.group(1)], s)
    return s


found = {}
# Только типы сообщений, которые обрабатывает майнер; другие (напр. reward-redeemed) могут
# содержать имена других зрителей и не нужны.
PUBSUB_TYPES = {"viewcount", "commercial", "stream-up", "stream-down", "points-earned",
                "claim-available", "claim-claimed", "active"}


def keep(name, value):
    found.setdefault(name, value)


user_id = None
for path in sorted(glob.glob(f"{trace_dir}/*.jsonl")):
    for line in open(path, encoding="utf-8"):
        r = json.loads(line)
        if r["src"] == "gql":
            for req, res in zip(r["request"], r["response"]):
                data = res.get("data") or {}
                op = req["op"]
                if op == "VideoPlayerStreamInfoOverlayChannel" and (data.get("user") or {}).get("stream"):
                    keep("gql_stream_info", data)
                elif op == "WithIsStreamLiveQuery" and (data.get("user") or {}).get("stream"):
                    keep("gql_stream_live", data)
                elif op == "RewardList":
                    state = data["channel"]["self"]["watchStreakMilestone"]["state"]
                    keep(f"gql_reward_list_{state.lower()}", data)
                elif op == "ChannelPointsContext":
                    keep("gql_channel_points", data)
        elif r["src"] == "pubsub" and r["ev"] == "in" and "topic" in r and isinstance(r["message"], dict):
            m = r["message"]
            if m.get("type") not in PUBSUB_TYPES:
                continue
            keep(f"pubsub_{m.get('type', 'unknown').replace('-', '_')}", {"topic": r["topic"], "message": m})
            if m.get("type") == "points-earned":
                user_id = m["data"]["point_gain"]["user_id"]
        elif r["src"] == "irc" and r["ev"] == "in" and " ROOMSTATE #" in r["line"]:
            keep("irc_roomstate", r["line"])

for name, value in sorted(found.items()):
    if isinstance(value, str):
        text = anonymize_text(value)
    else:
        if "topic" in value:  # pubsub: {"topic", "message"}
            value["topic"] = anonymize_topic(value["topic"])
        text = anonymize_text(json.dumps(anonymize(value), ensure_ascii=False, indent=2))
    if user_id:
        text = text.replace(user_id, "1000")
    ext = "txt" if isinstance(value, str) else "json"
    with open(f"{out_dir}/{name}.{ext}", "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print("wrote", f"{out_dir}/{name}.{ext}")
