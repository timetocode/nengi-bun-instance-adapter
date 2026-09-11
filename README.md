# nengi-bun-instance-adapter

Native Bun server adapter for nengi using `Bun.serve` and the
`nengi-dataviews` binary backend.

Keep the complete Nengi package family on one exact version:

```sh
bun add nengi@2.0.0-rc.127 \
    nengi-bun-instance-adapter@2.0.0-rc.127 \
    nengi-dataviews@2.0.0-rc.127
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
upgrade, a `400` response when it fails, and a `503` response when nengi refuses
admission. Admission is reserved before creating the native WebSocket and
released if upgrade fails; capacity refusals do not create sockets or queue
authentication-denial events.

The adapter bounds queued outbound data with `maxBufferedBytes` (4 MiB by
default). Crossing that limit terminates the socket and throws from `send`, so
nengi performs its normal immediate user and channel cleanup. Bun transport
idle timeout remains a longer fallback; nengi's handshake and Pong deadlines
own application liveness.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
connection lifecycle, timing, and deployment guidance.

Core connection, traffic and queue budgets also apply. Native receive limits
default to `instance.limits.maxPacketBytes`; an explicit adapter override does
not bypass the core limit. See the manual
[network limits](https://github.com/timetocode/nengi/blob/rc/2.0.0/docs/ai/network-limits.md)
for defaults and migration guidance.

On tested Bun 1.3.14, a server-initiated WebSocket close could leave
`server.stop(true)` unresolved after the socket close event, also reproducible
without nengi. Core user cleanup remains immediate. Check shutdown on your
deployment runtime; pre-upgrade admission refusals avoid creating that socket.

WebSocket text data is rejected. Incoming native Ping/Pong callbacks share the
core packet/byte traffic budget; they do not count as nengi clock replies or
refresh its liveness deadline.
