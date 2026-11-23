// Connect-клиент для бэкенда майнера. По сети идёт бинарный protobuf; URL
// относительный, поэтому работает и за dev-прокси Vite, и в продакшене.
// Вызовы из компонентов идут через TanStack Query (см. lib/query.ts).
import { createClient } from "@connectrpc/connect"
import { createConnectTransport } from "@connectrpc/connect-web"

import { MinerService } from "@/gen/miner/v1/miner_pb"

const transport = createConnectTransport({
  baseUrl: window.location.origin,
  useBinaryFormat: true,
})

export const rpc = createClient(MinerService, transport)
