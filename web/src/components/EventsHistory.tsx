// История событий (ListEvents) через TanStack Query: страницы назад по seq,
// отдельный кэш на каждый фильтр. При возврате на страницу кэш показывается
// сразу, а перезапрос идёт в фоне, только если данные устарели.
import { useInfiniteQuery } from "@tanstack/react-query"
import { useState } from "react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { errorText, keys } from "@/lib/query"
import { rpc } from "@/lib/rpc"
import { useLive } from "@/live/LiveProvider"
import { FeedItem } from "./FeedItem"

const PAGE = 50
const ALL = "__all__" // значение пункта «все каналы»

export function EventsHistory() {
  const { state } = useLive()
  const [channelId, setChannelId] = useState("")

  const q = useInfiniteQuery({
    queryKey: keys.events(channelId),
    queryFn: ({ pageParam, signal }) => rpc.listEvents({ beforeSeq: pageParam, channelId, limit: PAGE }, { signal }),
    initialPageParam: 0n,
    // Следующая страница — события старше последнего полученного.
    getNextPageParam: (last) => (last.events.length === PAGE ? last.events[last.events.length - 1].seq : undefined),
  })
  const events = q.data?.pages.flatMap((p) => p.events) ?? []

  return (
    <Card>
      <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-3">
        <CardTitle>История событий</CardTitle>
        <div className="flex gap-2">
          {/* Radix Select не принимает пустое значение, поэтому «все каналы» — это ALL. */}
          <Select value={channelId || ALL} onValueChange={(v) => setChannelId(v === ALL ? "" : v)}>
            <SelectTrigger className="w-48">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>все каналы</SelectItem>
              {[...state.channels]
                .sort((a, b) => a.displayName.localeCompare(b.displayName, "ru", { sensitivity: "base" }))
                .map((c) => (
                  <SelectItem key={c.id} value={c.id}>
                    {c.displayName}
                  </SelectItem>
                ))}
            </SelectContent>
          </Select>
          <Button variant="outline" onClick={() => q.refetch()} disabled={q.isFetching}>
            {q.isFetching && !q.isFetchingNextPage ? "Обновление…" : "Обновить"}
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {q.isError && <p className="text-sm text-red-600">Не удалось загрузить историю: {errorText(q.error)}</p>}
        {q.isPending && <p className="text-sm text-muted-foreground">Загрузка…</p>}
        {q.isSuccess && events.length === 0 && <p className="text-sm text-muted-foreground">Событий пока нет</p>}
        <div className="divide-y">
          {events.map((m) => (
            <FeedItem key={`${m.seq}`} msg={m} fullDate />
          ))}
        </div>
        {q.hasNextPage && (
          <Button variant="ghost" className="mt-3 w-full" onClick={() => q.fetchNextPage()} disabled={q.isFetchingNextPage}>
            {q.isFetchingNextPage ? "Загрузка…" : "Показать ещё"}
          </Button>
        )}
      </CardContent>
    </Card>
  )
}
