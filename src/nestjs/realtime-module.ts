/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument */
// NestJS DI uses any[] for injection tokens and factory args by design — mirrors mongo_queue's src/nestjs/index.ts.
import { randomUUID } from 'node:crypto';
import type { DynamicModule, OnApplicationShutdown, OnModuleInit, Provider } from '@nestjs/common';
import type { Db } from 'mongodb';
import { ChangeStreamHub, type ChangeStreamHubOptions } from '../core/change-stream-hub.js';
import { ConnectionRegistry } from '../presence/connection-registry.js';
import { PresenceStore } from '../presence/presence-store.js';
import { DEFAULT_PRESENCE_COLLECTION, type PresenceStoreOptions } from '../presence/types.js';
import { SocketServerHandle } from './socket-server-handle.js';

export const CHANGE_STREAM_HUB = 'MONGODB_REALTIME_HUB';
export const PRESENCE_STORE = 'MONGODB_REALTIME_PRESENCE_STORE';
export const CONNECTION_REGISTRY = 'MONGODB_REALTIME_CONNECTION_REGISTRY';
export const SERVER_ID = 'MONGODB_REALTIME_SERVER_ID';
export const REALTIME_ENABLED = 'MONGODB_REALTIME_ENABLED';
export const SOCKET_SERVER = 'MONGODB_REALTIME_SOCKET_SERVER';

export interface RealtimeModuleOptions {
    db: Db;
    presence?: Omit<PresenceStoreOptions, 'db' | 'serverId'>;
    hub?: ChangeStreamHubOptions;
    /**
     * Wire the hub's connection-driven watch parking off the module's
     * {@link ConnectionRegistry}. Default `true`: `hub.connectedKeys` is set
     * to `registry.connectedKeys`, overriding anything passed in
     * `options.hub.connectedKeys` (the registry is the intended source of
     * truth for "who is connected" once this module owns both). Set to
     * `false` to opt out of parking entirely, or to drive the hub from a
     * `connectedKeys` signal of your own passed via `options.hub` instead —
     * in that case `options.hub.connectedKeys` is used as-is (and may be
     * `undefined`, which keeps every stream open).
     */
    wireHubParking?: boolean;
    /**
     * Master switch, surfaced as the `REALTIME_ENABLED` token for pushers to
     * gate on. Default `true`. Typically wired from
     * `assertChangeStreamPreflight`'s `ok`, so one preflight decides for every
     * pusher whether realtime is live.
     */
    enabled?: boolean;
}

/**
 * Runs the presence/hub lifecycle Nest can't drive via decorators (this
 * package keeps `@nestjs/common` an optional peer, so no `@Injectable()`
 * anywhere — see `RealtimeModule.forRoot`, which wires this class in via a
 * factory provider instead of `useClass`).
 *
 * Teardown deliberately runs on `onApplicationShutdown`, NOT `onModuleDestroy`,
 * even though the latter sounds like the "right" hook for a module's own
 * cleanup. Nest runs the two as separate phases: it drains every provider's
 * `onModuleDestroy` across the whole app, THEN starts the `onApplicationShutdown`
 * phase. A hub-backed `ChangeStreamEventPusher` gateway (see
 * `change-stream-event-pusher.ts`) also tears down on `onApplicationShutdown` —
 * it signals its `shutdown` latch there and awaits its push loop draining. If
 * this class closed the hub on `onModuleDestroy` instead, the hub would close
 * (ending every hub-backed pusher's `for await` mid-stream) before those
 * pushers' own `onApplicationShutdown` ever set `shutdown`, so every pusher's
 * "watcher returned before shutdown" guard would fire as a spurious operator-
 * visible error on every clean shutdown — not a crash, just log noise that
 * looks like one.
 *
 * Putting both on `onApplicationShutdown` instead works IF this module's hub
 * closes after the gateways' loops have already been signalled to stop —
 * which is exactly what Nest gives you for free: within a single lifecycle
 * phase, hooks run in REVERSE module-instantiation order. So as long as
 * `RealtimeModule` is imported from the app's root module (making it one of
 * the first modules instantiated), its `onApplicationShutdown` runs LAST —
 * after every feature module's gateway has already signalled shutdown and
 * started draining. This mirrors fm's arrangement, where the shared
 * `TenantChangeStreamHub` lives in an early-instantiated `@Global` module for
 * the same reason (see the `whenShutdown` ordering comment in
 * `watch-entity-changes-all-tenants.ts`).
 *
 * Two obligations this places on the consumer: import `RealtimeModule` from
 * the ROOT module (not lazily from a feature module) so it instantiates
 * early, and call `app.enableShutdownHooks()` — without it Nest never invokes
 * `onApplicationShutdown` at all, on this class or on any gateway.
 */
export class RealtimeLifecycle implements OnModuleInit, OnApplicationShutdown {
    constructor(
        private readonly presenceStore: PresenceStore,
        private readonly hub: ChangeStreamHub
    ) {}

    async onModuleInit(): Promise<void> {
        await this.presenceStore.ensureIndexes();
    }

    async onApplicationShutdown(): Promise<void> {
        await this.presenceStore.clearServer();
        await this.hub.close();
    }
}

/**
 * The hub's `ignoreCollections` always includes the presence collection
 * currently in effect (spec §4.3's promised default), so every presence
 * `checkIn`/`keepAlive`/`checkOut`/`setPage` write doesn't force an
 * `updateLookup` + reader iteration on the hub's shared DB-level cursor for
 * events nobody there consumes. This is safe: `watchPresence` does NOT go
 * through the hub — it opens its own dedicated collection-level watch — so
 * ignoring the presence collection on the hub cannot break presence itself.
 * Any caller-supplied `options.hub.ignoreCollections` are preserved alongside
 * it, not replaced.
 */
function hubIgnoreCollections(options: RealtimeModuleOptions): string[] {
    return [options.presence?.collectionName ?? DEFAULT_PRESENCE_COLLECTION, ...(options.hub?.ignoreCollections ?? [])];
}

const OPTIONS_TOKEN = 'MONGODB_REALTIME_MODULE_OPTIONS';

const REALTIME_EXPORTS = [
    SERVER_ID,
    REALTIME_ENABLED,
    SOCKET_SERVER,
    CONNECTION_REGISTRY,
    PRESENCE_STORE,
    CHANGE_STREAM_HUB,
    RealtimeLifecycle,
];

/**
 * Adapts a factory written with named, typed parameters to the untyped
 * `(...args: any[])` shape Nest's `useFactory` requires, splitting the
 * options argument off the front on the async path and supplying the
 * closed-over options on the sync path. Keeps one code path for both entry
 * points WITHOUT hand-indexing into `args`.
 *
 * The cast at the boundary is confined to this one adapter — every provider
 * built with it keeps a normal, compiler-checked signature
 * `(options: RealtimeModuleOptions, ...deps) => T`, so a wrong `inject` token
 * or a copy-pasted factory that misuses a dependency's type still fails to
 * compile, the same protection the pre-refactor duplicated factories had.
 */
function adaptOptionsFactory<TDeps extends unknown[], TResult>(
    source: RealtimeModuleOptions | string,
    factory: (options: RealtimeModuleOptions, ...deps: TDeps) => TResult
): (...args: any[]) => TResult {
    if (typeof source === 'string') {
        return (...args: any[]): TResult => factory(args[0] as RealtimeModuleOptions, ...(args.slice(1) as TDeps));
    }
    return (...deps: any[]): TResult => factory(source, ...(deps as TDeps));
}

/**
 * The seven providers, built once for both `forRoot` and `forRootAsync`.
 *
 * `source` is either the options object itself (sync) or the DI token the
 * options are provided under (async). Providers that need the options are
 * built via {@link adaptOptionsFactory}, whose `inject` array is
 * `[...optionsInject, ...depTokens]` — the token order it requires to hand
 * back `(options, ...deps)` matches the factory signatures below 1:1.
 */
function buildProviders(source: RealtimeModuleOptions | string): Provider[] {
    const optionsInject = typeof source === 'string' ? [source] : [];

    return [
        { provide: SERVER_ID, useFactory: (): string => randomUUID() },
        {
            provide: REALTIME_ENABLED,
            useFactory: adaptOptionsFactory(source, (options): boolean => options.enabled !== false),
            inject: optionsInject,
        },
        { provide: SOCKET_SERVER, useFactory: (): SocketServerHandle => new SocketServerHandle() },
        { provide: CONNECTION_REGISTRY, useFactory: (): ConnectionRegistry => new ConnectionRegistry() },
        {
            provide: PRESENCE_STORE,
            useFactory: adaptOptionsFactory(
                source,
                (options, serverId: string): PresenceStore =>
                    new PresenceStore({ ...options.presence, db: options.db, serverId })
            ),
            inject: [...optionsInject, SERVER_ID],
        },
        {
            provide: CHANGE_STREAM_HUB,
            useFactory: adaptOptionsFactory(
                source,
                (options, registry: ConnectionRegistry): ChangeStreamHub =>
                    new ChangeStreamHub({
                        ...options.hub,
                        ignoreCollections: hubIgnoreCollections(options),
                        connectedKeys:
                            options.wireHubParking !== false ? registry.connectedKeys : options.hub?.connectedKeys,
                    })
            ),
            inject: [...optionsInject, CONNECTION_REGISTRY],
        },
        {
            provide: RealtimeLifecycle,
            useFactory: (presenceStore: PresenceStore, hub: ChangeStreamHub): RealtimeLifecycle =>
                new RealtimeLifecycle(presenceStore, hub),
            inject: [PRESENCE_STORE, CHANGE_STREAM_HUB],
        },
    ];
}

/**
 * Wires the four realtime primitives (server id, connection registry,
 * presence store, change-stream hub) plus their startup/shutdown lifecycle
 * into a Nest `DynamicModule`. Follows mongo_queue's `MongoQueueModule`
 * pattern: no decorators, string tokens, factory providers — so
 * `@nestjs/common` stays an optional peer dependency.
 */
export class RealtimeModule {
    static forRoot(options: RealtimeModuleOptions): DynamicModule {
        return { module: RealtimeModule, providers: buildProviders(options), exports: REALTIME_EXPORTS };
    }

    static forRootAsync(options: {
        imports?: unknown[];
        inject?: any[];
        useFactory: (...args: any[]) => RealtimeModuleOptions | Promise<RealtimeModuleOptions>;
    }): DynamicModule {
        return {
            module: RealtimeModule,
            imports: options.imports as DynamicModule['imports'],
            providers: [
                {
                    provide: OPTIONS_TOKEN,
                    useFactory: async (...args: any[]) => options.useFactory(...args),
                    inject: options.inject ?? [],
                },
                ...buildProviders(OPTIONS_TOKEN),
            ] as Provider[],
            exports: REALTIME_EXPORTS,
        };
    }
}
