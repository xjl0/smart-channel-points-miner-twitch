// Боковое меню: название, страницы и внизу — аккаунт и состояние live-соединения.
// На узком экране превращается в верхнюю полосу с горизонтальным меню.
import { History, LayoutDashboard, SlidersHorizontal, Tv, UserRound, type LucideIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { useLive, type ConnStatus } from "@/live/LiveProvider"

export type Page = "dashboard" | "channels" | "events" | "settings" | "account"

export const PAGES: { id: Page; label: string; icon: LucideIcon }[] = [
  { id: "dashboard", label: "Дашборд", icon: LayoutDashboard },
  { id: "channels", label: "Каналы", icon: Tv },
  { id: "events", label: "События", icon: History },
  { id: "settings", label: "Настройки", icon: SlidersHorizontal },
  { id: "account", label: "Аккаунт", icon: UserRound },
]

const statusView: Record<ConnStatus, { label: string; dot: string }> = {
  live: { label: "в эфире", dot: "bg-emerald-500" },
  connecting: { label: "подключение…", dot: "bg-amber-500 animate-pulse" },
  offline: { label: "нет связи", dot: "bg-red-500" },
}

export function Sidebar({ page }: { page: Page }) {
  const { state, status, lastSeq } = useLive()
  const view = statusView[status]
  return (
    <aside className="flex shrink-0 flex-col gap-3 border-b bg-muted/30 p-3 md:sticky md:top-0 md:h-screen md:w-56 md:border-r md:border-b-0 md:p-4">
      <div className="px-2 font-semibold leading-tight">
        Smart Twitch
        <br className="hidden md:block" /> Points Miner
      </div>

      <nav className="flex gap-1 overflow-x-auto md:flex-col">
        {PAGES.map(({ id, label, icon: Icon }) => (
          <a
            key={id}
            href={`#${id}`}
            className={cn(
              "flex items-center gap-2 whitespace-nowrap rounded-md px-3 py-2 text-sm transition-colors hover:bg-muted",
              page === id && "bg-muted font-medium",
            )}
          >
            <Icon className="size-4" />
            {label}
          </a>
        ))}
      </nav>

      <div className="mt-auto flex flex-wrap items-center gap-2 px-2 text-sm md:flex-col md:items-start">
        {state.account?.loggedIn ? (
          <Badge variant="secondary">@{state.account.login}</Badge>
        ) : (
          <Badge variant="destructive">не авторизован</Badge>
        )}
        <span className="flex items-center gap-2 text-muted-foreground" title={`seq ${lastSeq}`}>
          <span className={`size-2 rounded-full ${view.dot}`} />
          {view.label}
        </span>
      </div>
    </aside>
  )
}
