// Аккаунт Twitch: вход по device code и выход.
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { duration, secondsBetween, toDate } from "@/lib/format"
import { useCommand } from "@/lib/query"
import { rpc } from "@/lib/rpc"
import { useLive, useNow } from "@/live/LiveProvider"

export function AccountPanel() {
  const { state } = useLive()
  const now = useNow()
  const acc = state.account
  const pending = acc?.pendingLogin
  const login = useCommand("Вход", () => rpc.startLogin({}))
  const logout = useCommand("Выход", () => rpc.logout({}))

  return (
    <Card className="max-w-xl">
      <CardHeader>
        <CardTitle>Аккаунт Twitch</CardTitle>
        <CardDescription>
          {acc?.loggedIn ? `Вход выполнен как @${acc.login} (id ${acc.userId})` : "Вход не выполнен — майнер ничего не смотрит"}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {pending && (
          <div className="space-y-2 rounded-lg border p-4">
            <p className="text-sm">
              Откройте{" "}
              <a className="font-medium underline" href={pending.verificationUri} target="_blank" rel="noreferrer">
                {pending.verificationUri}
              </a>{" "}
              и введите код:
            </p>
            <p className="font-mono text-3xl font-bold tracking-widest">{pending.userCode}</p>
            <p className="text-xs text-muted-foreground">
              Код действует ещё {duration(Math.max(0, secondsBetween(now, toDate(pending.expiresAt))))}. Страница обновится сама.
            </p>
          </div>
        )}
        <div className="flex gap-2">
          <Button onClick={() => login.mutate(undefined)} disabled={login.isPending}>
            {acc?.loggedIn ? "Войти другим аккаунтом" : pending ? "Новый код" : "Войти через Twitch"}
          </Button>
          {acc?.loggedIn && (
            <Button variant="outline" onClick={() => logout.mutate(undefined)} disabled={logout.isPending}>
              Выйти
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
