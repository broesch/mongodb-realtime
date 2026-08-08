/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument */
// NestJS DI uses any[] for injection tokens and factory args by design — mirrors mongo_queue's src/nestjs/index.ts.
import { randomUUID } from 'node:crypto';
import type { DynamicModule, OnModuleDestroy, OnModuleInit, Provider } from '@nestjs/common';
import type { Db } from 'mongodb';
import { ChangeStreamHub, type ChangeStreamHubOptions } from '../core/change-stream-hub.js';
import { ConnectionRegistry } from '../presence/connection-registry.js';
import { PresenceStore } from '../presence/presence-store.js';
import type { PresenceStoreOptions } from '../presence/types.js';

export const CHANGE_STREAM_HUB = 'MONGODB_REALTIME_HUB';
export const PRESENCE_STORE = 'MONGODB_REALTIME_PRESENCE_STORE';
export const CONNECTION_REGISTRY = 'MONGODB_REALTIME_CONNECTION_REGISTRY';
export const SERVER_ID = 'MONGODB_REALTIME_SERVER_ID';

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
}

/**
 * Runs the presence/hub lifecycle Nest can't drive via decorators (this
 * package keeps `@nestjs/common` an optional peer, so no `@Injectable()`
 * anywhere — see `RealtimeModule.forRoot`, which wires this class in via a
 * factory provider instead of `useClass`).
 */
export class RealtimeLifecycle implements OnModuleInit, OnModuleDestroy {
    constructor(
        private readonly presenceStore: PresenceStore,
        private readonly hub: ChangeStreamHub
    ) {}

    async onModuleInit(): Promise<void> {
        await this.presenceStore.ensureIndexes();
    }

    async onModuleDestroy(): Promise<void> {
        await this.presenceStore.clearServer();
        await this.hub.close();
    }
}

function buildProviders(options: RealtimeModuleOptions): Provider[] {
    return [
        {
            provide: SERVER_ID,
            useFactory: (): string => randomUUID(),
        },
        {
            provide: CONNECTION_REGISTRY,
            useFactory: (): ConnectionRegistry => new ConnectionRegistry(),
        },
        {
            provide: PRESENCE_STORE,
            useFactory: (serverId: string): PresenceStore =>
                new PresenceStore({ ...options.presence, db: options.db, serverId }),
            inject: [SERVER_ID],
        },
        {
            provide: CHANGE_STREAM_HUB,
            useFactory: (registry: ConnectionRegistry): ChangeStreamHub =>
                new ChangeStreamHub({
                    ...options.hub,
                    connectedKeys:
                        options.wireHubParking !== false ? registry.connectedKeys : options.hub?.connectedKeys,
                }),
            inject: [CONNECTION_REGISTRY],
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
        return {
            module: RealtimeModule,
            providers: buildProviders(options),
            exports: [SERVER_ID, CONNECTION_REGISTRY, PRESENCE_STORE, CHANGE_STREAM_HUB, RealtimeLifecycle],
        };
    }

    static forRootAsync(options: {
        imports?: unknown[];
        inject?: any[];
        useFactory: (...args: any[]) => RealtimeModuleOptions | Promise<RealtimeModuleOptions>;
    }): DynamicModule {
        const OPTIONS_TOKEN = 'MONGODB_REALTIME_MODULE_OPTIONS';
        return {
            module: RealtimeModule,
            imports: options.imports as DynamicModule['imports'],
            providers: [
                {
                    provide: OPTIONS_TOKEN,
                    useFactory: async (...args: any[]) => options.useFactory(...args),
                    inject: options.inject ?? [],
                },
                {
                    provide: SERVER_ID,
                    useFactory: (): string => randomUUID(),
                },
                {
                    provide: CONNECTION_REGISTRY,
                    useFactory: (): ConnectionRegistry => new ConnectionRegistry(),
                },
                {
                    provide: PRESENCE_STORE,
                    useFactory: (moduleOptions: RealtimeModuleOptions, serverId: string): PresenceStore =>
                        new PresenceStore({ ...moduleOptions.presence, db: moduleOptions.db, serverId }),
                    inject: [OPTIONS_TOKEN, SERVER_ID],
                },
                {
                    provide: CHANGE_STREAM_HUB,
                    useFactory: (moduleOptions: RealtimeModuleOptions, registry: ConnectionRegistry): ChangeStreamHub =>
                        new ChangeStreamHub({
                            ...moduleOptions.hub,
                            connectedKeys:
                                moduleOptions.wireHubParking !== false
                                    ? registry.connectedKeys
                                    : moduleOptions.hub?.connectedKeys,
                        }),
                    inject: [OPTIONS_TOKEN, CONNECTION_REGISTRY],
                },
                {
                    provide: RealtimeLifecycle,
                    useFactory: (presenceStore: PresenceStore, hub: ChangeStreamHub): RealtimeLifecycle =>
                        new RealtimeLifecycle(presenceStore, hub),
                    inject: [PRESENCE_STORE, CHANGE_STREAM_HUB],
                },
            ] as Provider[],
            exports: [SERVER_ID, CONNECTION_REGISTRY, PRESENCE_STORE, CHANGE_STREAM_HUB, RealtimeLifecycle],
        };
    }
}
