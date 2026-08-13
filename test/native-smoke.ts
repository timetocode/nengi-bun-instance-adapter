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
const instance = new Instance(context)
const adapter = new BunInstanceAdapter(instance.network, { path: '/nengi' })
const client = new Client(context, WebSocketClientAdapter, 20)
client.setDisconnectHandler(() => {})
instance.onConnect = async handshake => ({ echoed: handshake.runtime })
const port = 20_000 + Math.floor(Math.random() * 20_000)

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

    console.log('bun native adapter smoke ok')
} finally {
    client.disconnect('native smoke cleanup')
    await adapter.close()
}
