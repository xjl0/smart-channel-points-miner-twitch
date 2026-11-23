#!/usr/bin/env python3
"""Сводка по сырой трассе трафика Twitch (MINER_TRACE_DIR, по умолчанию ./trace).

Запуск: python3 scripts/trace_report.py [trace_dir] [--channel <channel_id>]

Выводит счётчики по источникам, топикам PubSub и типам сообщений, IRC-командам,
GQL-операциям, а (с --channel) хронологию стрика из RewardList и
события баллов одного канала.
"""
import collections
import glob
import json
import re
import sys


def records(trace_dir):
    for path in sorted(glob.glob(f"{trace_dir}/*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)


def main():
    args = sys.argv[1:]
    channel = None
    if "--channel" in args:
        i = args.index("--channel")
        channel = args[i + 1]
        del args[i : i + 2]
    trace_dir = args[0] if args else "trace"

    per_src, topics, frames, irc, gql = (collections.Counter() for _ in range(5))
    streak, points = [], []
    first = last = None
    for r in records(trace_dir):
        first, last = first or r["ts"], r["ts"]
        per_src[(r["src"], r["ev"])] += 1
        if r["src"] == "pubsub" and r["ev"] == "in":
            if "topic" in r:
                msg = r["message"] if isinstance(r["message"], dict) else {}
                topics[(r["topic"].split(".")[0], msg.get("type"))] += 1
                gain = (msg.get("data") or {}).get("point_gain") or {}
                if channel and gain.get("channel_id") == channel:
                    points.append((r["ts"], gain.get("reason_code"), gain.get("total_points")))
            else:
                fr = r["frame"]
                frames[(fr.get("type"), fr.get("error") or "")] += 1
        elif r["src"] == "irc" and r["ev"] == "in":
            line = re.sub(r"^@\S+ ", "", r["line"])
            parts = line.split(" ")
            irc[parts[1] if line.startswith(":") and len(parts) > 1 else parts[0]] += 1
        elif r["src"] == "gql":
            ops = [o["op"] for o in r["request"]]
            gql[",".join(sorted(set(ops)))] += 1
            for i, op in enumerate(ops):
                if op == "RewardList" and r["request"][i]["vars"].get("channelID") == channel:
                    m = r["response"][i]["data"]["channel"]["self"]["watchStreakMilestone"]
                    streak.append((r["ts"], m["state"], m["watchStreakMilestone"]["value"],
                                   m["watchStreakMilestone"]["achievementTimestamp"]))

    print(f"trace {first} … {last}")
    print("\nby source:")
    for k, v in per_src.most_common():
        print(f"  {k[0]:7} {k[1]:15} {v}")
    print("\npubsub control frames:", dict(frames))
    print("\npubsub messages (topic, type):")
    for k, v in topics.most_common():
        print(f"  {k[0]:32} {str(k[1]):28} {v}")
    print("\nirc commands:", dict(irc.most_common()))
    print("\ngql operations:", dict(gql.most_common()))
    if channel:
        print(f"\nRewardList timeline for {channel}:")
        prev = None
        for ts, state, value, at in streak:
            if (state, value, at) != prev:
                print(f"  {ts}  state={state} value={value} achieved={at}")
                prev = (state, value, at)
        print(f"\npoints for {channel}:")
        for p in points:
            print("  ", *p)


if __name__ == "__main__":
    main()
