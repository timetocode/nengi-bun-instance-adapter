import { User } from 'nengi'
import type {
    BinaryAdapter,
    BinaryPayload,
    IServerNetworkAdapter,
    InstanceNetwork
} from 'nengi'
import { dataViewBinary } from 'nengi-dataviews'

const DEFAULT_MAX_BUFFERED_BYTES = 4 * 1024 * 1024
const DEFAULT_IDLE_TIMEOUT_SECONDS = 120

export type BunSocketAddress = {
    address: string
    port: number
}

export interface BunServerWebSocket<Data = unknown> {
    readonly data: Data
    readonly readyState: number
    readonly remoteAddress?: string
    send(payload: BinaryPayload, compress?: boolean): number
    close(code?: number, reason?: string): void
    terminate(): void
    getBufferedAmount(): number
}

export interface BunServer<Data = unknown> {
    readonly port: number
    readonly hostname: string
    upgrade(request: Request, options: { data: Data }): boolean
    requestIP(request: Request): BunSocketAddress | null
    stop(closeActiveConnections?: boolean): Promise<void>
}

export type BunWebSocketHandler<Data = unknown> = {
    open?(socket: BunServerWebSocket<Data>): void
    message?(socket: BunServerWebSocket<Data>, message: string | Uint8Array): void
    close?(socket: BunServerWebSocket<Data>, code: number, reason: string): void
    error?(socket: BunServerWebSocket<Data>, error: Error): void
    maxPayloadLength?: number
    backpressureLimit?: number
    closeOnBackpressureLimit?: boolean
    idleTimeout?: number
    sendPings?: boolean
    perMessageDeflate?: boolean
}

export type BunListenOptions = number | {
    port: number
    hostname?: string
    path?: string
}

export type BunInstanceAdapterConfig = {
    binary?: BinaryAdapter<BinaryPayload, ArrayBuffer>
    path?: string
    maxBufferedBytes?: number
    maxPayloadLength?: number
    idleTimeoutSeconds?: number
    perMessageDeflate?: boolean
}

export type BunInstanceSocketData = {
    adapter: BunInstanceAdapter
    user: User | null
    remoteAddress: string | null
}

type BunServeOptions<Data> = {
    port: number
    hostname?: string
    fetch(request: Request, server: BunServer<Data>): Response | Promise<Response> | undefined
    websocket: BunWebSocketHandler<Data>
}

type BunRuntime = {
    serve<Data>(options: BunServeOptions<Data>): BunServer<Data>
}

function getBunRuntime(): BunRuntime {
    const runtime = (globalThis as unknown as { Bun?: BunRuntime }).Bun
    if (!runtime) {
        throw new Error('nengi-bun-instance-adapter requires the Bun runtime.')
    }
    return runtime
}

function closeReason(reason: unknown) {
    const serialized = typeof reason === 'string'
        ? reason
        : JSON.stringify(reason ?? 'closed')
    const value = serialized ?? 'closed'
    let clipped = value.slice(0, 123)
    while (new TextEncoder().encode(clipped).byteLength > 123) {
        clipped = clipped.slice(0, -1)
    }
    return clipped
}

function requestPath(request: Request) {
    return new URL(request.url).pathname
}

function isBinaryPayload(value: unknown): value is BinaryPayload {
    return value instanceof ArrayBuffer || ArrayBuffer.isView(value)
}

export class BunInstanceAdapter implements IServerNetworkAdapter<BinaryPayload, ArrayBuffer, BunListenOptions> {
    readonly network: InstanceNetwork
    readonly binary: BinaryAdapter<BinaryPayload, ArrayBuffer>
    readonly websocket: BunWebSocketHandler<BunInstanceSocketData>
    server: BunServer<BunInstanceSocketData> | null = null

    private readonly path: string
    private readonly maxBufferedBytes: number

    constructor(network: InstanceNetwork, config: BunInstanceAdapterConfig = {}) {
        this.network = network
        this.binary = config.binary ?? dataViewBinary
        this.path = config.path ?? '/'
        this.maxBufferedBytes = config.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES

        if (!Number.isFinite(this.maxBufferedBytes) || this.maxBufferedBytes <= 0) {
            throw new Error('BunInstanceAdapter maxBufferedBytes must be greater than zero.')
        }

        this.websocket = {
            maxPayloadLength: config.maxPayloadLength ?? 16 * 1024 * 1024,
            backpressureLimit: this.maxBufferedBytes,
            closeOnBackpressureLimit: true,
            idleTimeout: config.idleTimeoutSeconds ?? DEFAULT_IDLE_TIMEOUT_SECONDS,
            perMessageDeflate: config.perMessageDeflate ?? false,
            open: socket => this.onOpen(socket),
            message: (socket, message) => this.onMessage(socket, message),
            close: (socket, code, reason) => this.onClose(socket, reason),
            error: (socket, error) => this.onError(socket, error)
        }
    }

    listen(options: BunListenOptions, ready?: () => void) {
        if (this.server) {
            throw new Error('BunInstanceAdapter is already listening.')
        }
        const listenOptions = typeof options === 'number' ? { port: options } : options
        const path = listenOptions.path ?? this.path
        this.server = getBunRuntime().serve({
            port: listenOptions.port,
            hostname: listenOptions.hostname,
            websocket: this.websocket,
            fetch: (request, server) => {
                if (requestPath(request) !== path) {
                    return new Response('Not found.', { status: 404 })
                }
                if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
                    return new Response('WebSocket upgrade required.', { status: 426 })
                }
                return this.upgrade(request, server)
            }
        })
        ready?.()
    }

    upgrade(request: Request, server: BunServer<BunInstanceSocketData>): Response | undefined {
        const remoteAddress = server.requestIP(request)?.address ?? null
        const accepted = server.upgrade(request, {
            data: { adapter: this, user: null, remoteAddress }
        })
        return accepted ? undefined : new Response('WebSocket upgrade failed.', { status: 400 })
    }

    send(user: User, payload: ArrayBuffer) {
        const socket = user.socket as BunServerWebSocket<BunInstanceSocketData>
        if (socket.readyState !== 1) {
            throw new Error('Cannot send a nengi snapshot on a closed Bun WebSocket.')
        }
        if (socket.getBufferedAmount() + payload.byteLength > this.maxBufferedBytes) {
            socket.terminate()
            throw new Error(`Bun WebSocket backpressure exceeded ${this.maxBufferedBytes} bytes.`)
        }

        const result = socket.send(payload, false)
        if (result === 0) {
            socket.terminate()
            throw new Error('Bun WebSocket rejected a nengi snapshot.')
        }
        if (socket.getBufferedAmount() > this.maxBufferedBytes) {
            socket.terminate()
            throw new Error(`Bun WebSocket backpressure exceeded ${this.maxBufferedBytes} bytes.`)
        }
    }

    disconnect(user: User, reason: unknown) {
        const socket = user.socket as BunServerWebSocket<BunInstanceSocketData>
        socket.close(1000, closeReason(reason))
    }

    terminate(user: User) {
        const socket = user.socket as BunServerWebSocket<BunInstanceSocketData>
        socket.terminate()
    }

    async close() {
        const server = this.server
        this.server = null
        await server?.stop(true)
    }

    private onOpen(socket: BunServerWebSocket<BunInstanceSocketData>) {
        const data = socket.data
        const user = new User(socket, data.adapter)
        data.user = user
        user.remoteAddress = data.remoteAddress ?? socket.remoteAddress ?? null
        data.adapter.network.onOpen(user)
    }

    private onMessage(socket: BunServerWebSocket<BunInstanceSocketData>, message: string | Uint8Array) {
        const { adapter, user } = socket.data
        if (!user) {
            return
        }
        if (!isBinaryPayload(message)) {
            adapter.network.notifyInboundMessageError(
                user,
                new Uint8Array(),
                new Error('Nengi requires binary WebSocket messages.')
            )
            adapter.network.disconnectMalformedInboundUser(user)
            return
        }
        adapter.network.onMessage(user, message)
    }

    private onClose(socket: BunServerWebSocket<BunInstanceSocketData>, reason: string) {
        const { adapter, user } = socket.data
        if (!user) {
            return
        }
        socket.data.user = null
        adapter.network.onClose(user, reason)
    }

    private onError(socket: BunServerWebSocket<BunInstanceSocketData>, error: Error) {
        const { adapter, user } = socket.data
        if (!user) {
            return
        }
        socket.data.user = null
        adapter.network.onClose(user, error)
        socket.terminate()
    }
}
