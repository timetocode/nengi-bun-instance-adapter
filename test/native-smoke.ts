import {
    Binary,
    Client,
    Context,
    defineMessageSchema,
    Instance,
    NetworkEvent,
    User
} from 'nengi'
import { WebSocketClientAdapter } from 'nengi-websocket-client-adapter'
import { BunInstanceAdapter } from '../build/index.js'

function withTimeout<T>(promise: Promise<T>, label: string) {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error(`${label} timed out.`)), 2_000)
        })
    ])
}

async function waitForQueueEvent(instance: Instance, type: NetworkEvent, label: string) {
    const deadline = performance.now() + 1_000
    while (performance.now() < deadline) {
        while (!instance.queue.isEmpty()) {
            const event = instance.queue.next()
            if (event.type === type) {
                return event
            }
        }
        await Bun.sleep(5)
    }
    throw new Error(`${label} timed out.`)
}

const context = new Context()
context.register(1, defineMessageSchema({ value: Binary.UInt8 }))
const instance = new Instance(context, { limits: { maxConnections: 1 } })
const adapter = new BunInstanceAdapter(instance.network, { path: '/nengi' })
if (instance.limits.maxPacketBytes !== 65536) throw new Error('Native smoke resolved a stale core package.')
const rejectedLimits: string[] = []
instance.onNetworkLimit = event => rejectedLimits.push(event.limit)
const client = new Client(context, WebSocketClientAdapter, 20)
client.setDisconnectHandler(() => {})
instance.onConnect = async handshake => ({ echoed: handshake.runtime })
const port = 20_000 + Math.floor(Math.random() * 20_000)

// Failed HTTP upgrades must return their pending admission reservation.
for (const throws of [false, true]) {
    const failure = new Error('test upgrade failure')
    const server = {
        requestIP: () => null,
        upgrade: () => { if (throws) throw failure; return false }
    } as unknown as Parameters<BunInstanceAdapter['upgrade']>[1]
    try {
        const response = adapter.upgrade(new Request('http://localhost/nengi'), server)
        if (throws || response?.status !== 400) throw new Error('Unexpected upgrade result.')
    } catch (error) {
        if (error !== failure) throw error
    }
    if (instance.network.pendingUsers.size !== 0 || instance.queue.length !== 0) {
        throw new Error('Failed upgrade retained admission or lifecycle work.')
    }
}

{
    let terminated = false
    const boundedAdapter = new BunInstanceAdapter(instance.network, { maxBufferedBytes: 8 })
    const socket = {
        readyState: 1,
        getBufferedAmount: () => 4,
        terminate: () => { terminated = true }
    }
    const user = new User(socket, boundedAdapter)
    try {
        boundedAdapter.send(user, new ArrayBuffer(8))
        throw new Error('Expected Bun backpressure to reject the send.')
    } catch (error) {
        if (!terminated) {
            throw new Error('Bun backpressure did not terminate the socket.')
        }
    }
}

await new Promise<void>(resolve => {
    adapter.listen({ port, hostname: '127.0.0.1' }, resolve)
})

try {
    if (!adapter.server?.port) {
        throw new Error('The native Bun server did not expose its assigned port.')
    }
    const httpResponse = await fetch(`http://127.0.0.1:${adapter.server.port}/nengi`)
    await httpResponse.text()
    if (httpResponse.status !== 426) {
        throw new Error(`Expected the Bun route to require WebSocket upgrade, got ${httpResponse.status}.`)
    }

    const result = await withTimeout(
        client.connect(`ws://127.0.0.1:${adapter.server.port}/nengi`, {
            runtime: 'bun-native'
        }),
        'Native Bun nengi handshake'
    )
    if (!result.accepted) {
        throw new Error('Native Bun handshake was denied.')
    }

    const connected = await waitForQueueEvent(instance, NetworkEvent.UserConnected, 'Bun connection event')
    if (connected.payload?.echoed !== 'bun-native') {
        throw new Error('Native Bun connection payload was not preserved.')
    }

    const refusedUpgrade = await fetch(`http://127.0.0.1:${adapter.server.port}/nengi`, {
        headers: { Upgrade: 'websocket' }
    })
    await refusedUpgrade.text()
    if (refusedUpgrade.status !== 503) throw new Error('Capacity refusal must precede native upgrade.')

    const refusedClient = new Client(context, WebSocketClientAdapter, 20)
    refusedClient.setDisconnectHandler(() => {})
    refusedClient.setWebsocketErrorHandler(() => {})
    let refused = false
    try {
        refused = await withTimeout(
            refusedClient.connect(`ws://127.0.0.1:${adapter.server.port}/nengi`, {}).then(() => false, () => true),
            'Admission refusal'
        )
    } finally {
        refusedClient.disconnect('refusal cleanup')
    }
    if (!refused || !rejectedLimits.includes('maxConnections') || instance.users.size !== 1) {
        throw new Error('Admission refusal did not preserve the established connection.')
    }

    instance.step()
    await Bun.sleep(10)
    const frames = client.network.drainFrames()
    if (frames.length !== 1 || frames[0].tick !== 1) {
        throw new Error(`Expected native Bun snapshot tick 1, received ${frames.length}.`)
    }

    const user = [...instance.users.values()][0]
    if (!user || user.clockSyncSamples < 1 || user.pendingPings.size !== 0) {
        throw new Error('Native Bun immediate ping/pong did not complete.')
    }

    const nativeControls: number[] = []
    const admitControl = instance.network.onTransportControl.bind(instance.network)
    instance.network.onTransportControl = (user, bytes) => { nativeControls.push(bytes); return admitControl(user, bytes) }
    const nativeSocket = client.adapter.socket as WebSocket & { ping(data: string): void, pong(data: string): void }
    nativeSocket.ping('ping')
    nativeSocket.pong('pong')
    await Bun.sleep(10)
    if (nativeControls.length !== 2 || nativeControls.some(bytes => bytes !== 4)) {
        throw new Error('Native Bun control frames bypassed traffic accounting.')
    }

    client.addCommand({ ntype: 1, value: 37 })
    client.flush()
    const command = await waitForQueueEvent(instance, NetworkEvent.CommandSet, 'Bun command delivery')
    if (command.commands?.length !== 1 || command.commands[0].value !== 37) {
        throw new Error('Native Bun command payload was not preserved.')
    }

    client.disconnect('native smoke complete')
    await waitForQueueEvent(instance, NetworkEvent.UserDisconnected, 'Bun disconnect lifecycle')
    if (instance.users.size !== 0) {
        throw new Error('Native Bun disconnect did not remove the user.')
    }

} finally {
    client.disconnect('native smoke cleanup')
    await withTimeout(adapter.close(), 'Bun server shutdown')
}
console.log('bun native adapter smoke ok')
