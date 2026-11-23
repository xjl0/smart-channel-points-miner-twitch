// Корень админ-панели: боковое меню слева и выбранная страница на всю ширину.
// Страница хранится в адресе (#channels), поэтому переживает перезагрузку.
// Все данные приходят из live-потока.
import { useEffect, useState } from "react"

import { AccountPanel } from "@/components/AccountPanel"
import { ChannelsTable } from "@/components/ChannelsTable"
import { ChatPanel } from "@/components/ChatPanel"
import { EventsHistory } from "@/components/EventsHistory"
import { LiveFeed } from "@/components/LiveFeed"
import { QueueCard } from "@/components/QueueCard"
import { SettingsPage } from "@/components/SettingsPage"
import { PAGES, Sidebar, type Page } from "@/components/Sidebar"
import { SlotsPanel } from "@/components/SlotsPanel"
import { Toaster } from "@/components/ui/sonner"
import { LiveProvider } from "@/live/LiveProvider"

/** Текущая страница из адреса; неизвестный адрес — дашборд. */
function pageFromHash(): Page {
  const id = window.location.hash.slice(1)
  return PAGES.some((p) => p.id === id) ? (id as Page) : "dashboard"
}

function usePage(): Page {
  const [page, setPage] = useState(pageFromHash)
  useEffect(() => {
    const onHash = () => setPage(pageFromHash())
    window.addEventListener("hashchange", onHash)
    return () => window.removeEventListener("hashchange", onHash)
  }, [])
  return page
}

export default function App() {
  const page = usePage()
  return (
    <LiveProvider>
      <div className="flex min-h-screen flex-col bg-background text-foreground md:flex-row">
        <Sidebar page={page} />
        <main className="min-w-0 flex-1 space-y-4 p-4 sm:p-6">
          {page === "dashboard" && (
            <>
              <SlotsPanel />
              <ChatPanel />
              <QueueCard />
              <LiveFeed />
            </>
          )}
          {page === "channels" && <ChannelsTable />}
          {page === "events" && <EventsHistory />}
          {page === "settings" && <SettingsPage />}
          {page === "account" && <AccountPanel />}
        </main>
      </div>
      <Toaster />
    </LiveProvider>
  )
}
