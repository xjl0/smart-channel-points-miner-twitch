#!/usr/bin/env python3
"""Проверить жёсткие лимиты на записанном трафике (MINER_TRACE_DIR, по умолчанию ./trace):
не более 2 чатов одновременно (по отправленным нами IRC JOIN/PART, со сбросом
при каждом переподключении) и не более 2 одновременно просматриваемых каналов.

Сессия просмотра канала — серия маячков minute-watched с интервалом меньше
WINDOW, охватывающая [первый маячок, последний маячок]; проверка считает, сколько
сессий пересекается в каждый момент (переключение A→B не пересечение).

Запуск: python3 scripts/check_limits.py [trace_dir]
"""
import glob
import json
import sys
from datetime import datetime

LIMIT = 2
WINDOW = 75  # секунды; один маячок на канал примерно раз в 60 с


def ts(s):
    return datetime.fromisoformat(s[:26].rstrip("Z")).timestamp()


def main():
    trace_dir = sys.argv[1] if len(sys.argv) > 1 else "trace"
    joined, max_joined, max_at = set(), 0, ""
    beacons = []
    for path in sorted(glob.glob(f"{trace_dir}/*.jsonl")):
        for line in open(path, encoding="utf-8"):
            r = json.loads(line)
            if r["src"] == "irc" and r["ev"] == "out":
                cmd, _, arg = r["line"].partition(" ")
                if cmd == "NICK":  # новое соединение: состояние сервера начинается с пустого
                    joined = set()
                elif cmd == "JOIN":
                    joined.add(arg.lstrip("#"))
                elif cmd == "PART":
                    joined.discard(arg.lstrip("#"))
                if len(joined) > max_joined:
                    max_joined, max_at = len(joined), r["ts"]
            elif r["src"] == "spade":
                beacons.append((ts(r["ts"]), r["properties"]["channel"]))

    # Строим сессии по каналам из серий маячков.
    sessions, last = [], {}
    for t, c in beacons:
        if c in last and t - last[c][1] <= WINDOW:
            last[c][1] = t
        else:
            if c in last:
                sessions.append((last[c][0], last[c][1], c))
            last[c] = [t, t]
    sessions += [(a, b, c) for c, (a, b) in last.items()]
    # Проход: +1 в начале, -1 сразу после конца.
    points = sorted([(a, 1, c) for a, b, c in sessions] + [(b + 0.001, -1, c) for a, b, c in sessions])
    cur, worst, worst_at = 0, 0, ""
    for t, d, _ in points:
        cur += d
        if cur > worst:
            worst, worst_at = cur, datetime.fromtimestamp(t).isoformat()
    print(f"chats joined at once: max {max_joined} (at {max_at}) -> {'OK' if max_joined <= LIMIT else 'VIOLATION'}")
    print(f"channels watched at once: max {worst} (at {worst_at}) -> {'OK' if worst <= LIMIT else 'VIOLATION'}")
    print(f"currently joined: {sorted(joined)}; beacons total: {len(beacons)}")


if __name__ == "__main__":
    main()
