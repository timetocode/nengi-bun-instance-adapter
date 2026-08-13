# nengi-bun-instance-adapter

Native Bun server adapter for nengi using `Bun.serve` and the
`nengi-dataviews` binary backend.

Keep the complete Nengi package family on one exact version:

```sh
bun add nengi@2.0.0-rc.126 \
    nengi-bun-instance-adapter@2.0.0-rc.126 \
    nengi-dataviews@2.0.0-rc.126
```

```ts
import { Context, Instance } from 'nengi'
import { BunInstanceAdapter } from 'nengi-bun-instance-adapter'

const context = new Context()
const instance = new Instance(context)
const adapter = new BunInstanceAdapter(instance.network)

adapter.listen({ port: 8079, hostname: '0.0.0.0' })
```

To share an existing `Bun.serve` application, route a request through
`adapter.upgrade(request, server)` and install `adapter.websocket` as the
server's WebSocket handler. `upgrade` returns `undefined` when Bun accepts the
upgrade and a `400` response when it does not.

The adapter bounds queued outbound data with `maxBufferedBytes` (4 MiB by
default). Crossing that limit terminates the socket and throws from `send`, so
nengi performs its normal immediate user and channel cleanup. Bun transport
idle timeout remains a longer fallback; nengi's handshake and Pong deadlines
own application liveness.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
connection lifecycle, timing, and deployment guidance.
