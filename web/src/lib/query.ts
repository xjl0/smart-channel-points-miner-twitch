// TanStack Query: кэш запросов и единая обработка команд админки.
//
// Запросов «на чтение» у админки мало: почти всё состояние приходит из
// live-потока (SSE). Через кэш идёт история событий (ListEvents), а команды
// (добавить канал, сохранить настройки и т.п.) — через useCommand.
import { QueryClient, useMutation, useQueryClient } from "@tanstack/react-query"
import { ConnectError } from "@connectrpc/connect"
import { toast } from "sonner"

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // При возврате на страницу кэш показывается сразу; перезапрос в фоне —
      // только если данные старше этого.
      staleTime: 30_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
})

/** Ключи кэша. */
export const keys = {
  events: (channelId: string) => ["events", channelId] as const,
  allEvents: ["events"] as const,
}

/** Текст ошибки RPC для пользователя. */
export const errorText = (err: unknown) => (err instanceof ConnectError ? err.rawMessage : String(err))

/**
 * Команда админки: RPC с toast-ом при ошибке (и при успехе, если задан
 * success). После успеха история событий помечается устаревшей — команда
 * могла добавить в неё запись. Само состояние обновит live-поток.
 */
export function useCommand<TArgs, TResult>(
  action: string,
  fn: (args: TArgs) => Promise<TResult>,
  success?: string | ((r: TResult) => string),
) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: fn,
    onError: (err) => toast.error(`${action}: ${errorText(err)}`),
    onSuccess: (r) => {
      if (success) toast.success(typeof success === "string" ? success : success(r))
      void qc.invalidateQueries({ queryKey: keys.allEvents })
    },
  })
}
