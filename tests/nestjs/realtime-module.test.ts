import { vi } from 'vitest';
import { ChangeStreamHub } from '../../src/core/change-stream-hub.js';
import { ConnectionRegistry } from '../../src/presence/connection-registry.js';
import { PresenceStore } from '../../src/presence/presence-store.js';
import {
    CHANGE_STREAM_HUB,
    CONNECTION_REGISTRY,
    PRESENCE_STORE,
    REALTIME_ENABLED,
    RealtimeLifecycle,
    RealtimeModule,
    SERVER_ID,
} from '../../src/nestjs/realtime-module.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

interface FactoryProvider {
    provide: unknown;
    useFactory: (...args: unknown[]) => unknown;
    inject?: unknown[];
}

/**
 * Manually resolves a DynamicModule's factory providers in declaration order
 * (each provider only depends on earlier ones) — no full Nest app needed
 * since every provider in `RealtimeModule` is a plain factory. `useFactory`
 * results are awaited (Nest itself awaits factory results too), because
 * `forRootAsync`'s options-resolving provider is declared `async`.
 */
async function resolveProviders(providers: FactoryProvider[]): Promise<Map<unknown, unknown>> {
    const resolved = new Map<unknown, unknown>();
    for (const provider of providers) {
        const args = (provider.inject ?? []).map(token => resolved.get(token));
        resolved.set(provider.provide, await provider.useFactory(...args));
    }
    return resolved;
}

function connectedKeysOf(hub: ChangeStreamHub): unknown {
    return (hub as unknown as { options: { connectedKeys: unknown } }).options.connectedKeys;
}

function ignoreCollectionsOf(hub: ChangeStreamHub): string[] {
    return [...(hub as unknown as { ignoreCollections: Set<string> }).ignoreCollections];
}

describe('RealtimeModule.forRoot', () => {
    afterAll(() => stopReplSet());

    it('wires SERVER_ID as a uuid and every token to the right class', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);

        const serverId = resolved.get(SERVER_ID);
        expect(typeof serverId).toBe('string');
        expect(serverId as string).toMatch(UUID_RE);

        const registry = resolved.get(CONNECTION_REGISTRY);
        expect(registry).toBeInstanceOf(ConnectionRegistry);

        const presenceStore = resolved.get(PRESENCE_STORE) as PresenceStore;
        expect(presenceStore).toBeInstanceOf(PresenceStore);
        expect(presenceStore.serverId).toBe(serverId);

        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;
        expect(hub).toBeInstanceOf(ChangeStreamHub);

        expect(resolved.get(RealtimeLifecycle)).toBeInstanceOf(RealtimeLifecycle);

        await hub.close();
    });

    it('wireHubParking undefined (default): hub.connectedKeys is wired to the registry, overriding any hub.connectedKeys passed in', async () => {
        const { db } = await startReplSet();
        const suppliedSignal = new ConnectionRegistry().connectedKeys;
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
            hub: { connectedKeys: suppliedSignal },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        const registry = resolved.get(CONNECTION_REGISTRY) as ConnectionRegistry;
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;

        expect(connectedKeysOf(hub)).toBe(registry.connectedKeys);
        expect(connectedKeysOf(hub)).not.toBe(suppliedSignal);

        await hub.close();
    });

    it('wireHubParking: false keeps the hub on its own supplied connectedKeys', async () => {
        const { db } = await startReplSet();
        const suppliedSignal = new ConnectionRegistry().connectedKeys;
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
            hub: { connectedKeys: suppliedSignal },
            wireHubParking: false,
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        const registry = resolved.get(CONNECTION_REGISTRY) as ConnectionRegistry;
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;

        expect(connectedKeysOf(hub)).toBe(suppliedSignal);
        expect(connectedKeysOf(hub)).not.toBe(registry.connectedKeys);

        await hub.close();
    });

    it('wireHubParking: false with no hub.connectedKeys supplied leaves the hub undefined (every stream stays open)', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
            wireHubParking: false,
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;

        expect(connectedKeysOf(hub)).toBeUndefined();

        await hub.close();
    });

    it("default wiring: the hub's ignoreCollections contains the default presence collection name ('onlineUsers')", async () => {
        const { db } = await startReplSet();
        // No `presence` option at all: `hubIgnoreCollections` falls back to
        // the PresenceStore's own default ('onlineUsers') — verified by the
        // dedicated custom-name test below for the overridden case.
        const dynamicModule = RealtimeModule.forRoot({ db });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;

        expect(ignoreCollectionsOf(hub)).toContain('onlineUsers');

        await hub.close();
    });

    it('custom presence.collectionName: the hub ignores that name instead of the default', async () => {
        const { db } = await startReplSet();
        const customName = uniqueCollection('presence');
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: customName },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;

        expect(ignoreCollectionsOf(hub)).toContain(customName);
        expect(ignoreCollectionsOf(hub)).not.toContain('onlineUsers');

        await hub.close();
    });

    it('user-supplied hub.ignoreCollections are preserved alongside the presence collection', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            hub: { ignoreCollections: ['auditLog', 'metrics'] },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;

        expect(ignoreCollectionsOf(hub)).toEqual(expect.arrayContaining(['auditLog', 'metrics', 'onlineUsers']));

        await hub.close();
    });

    it('the lifecycle provider runs ensureIndexes on init and clearServer + hub.close on onApplicationShutdown', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);

        const presenceStore = resolved.get(PRESENCE_STORE) as PresenceStore;
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;
        const lifecycle = resolved.get(RealtimeLifecycle) as RealtimeLifecycle;

        // Teardown must be wired to onApplicationShutdown, NOT onModuleDestroy —
        // see the doc comment on RealtimeLifecycle for why (Nest runs ALL
        // onModuleDestroy hooks before ANY onApplicationShutdown hook; closing
        // the hub on the earlier phase would end hub-backed pushers' loops
        // before those pushers' own onApplicationShutdown had signalled them
        // to stop).
        expect(typeof lifecycle.onApplicationShutdown).toBe('function');
        expect((lifecycle as unknown as { onModuleDestroy?: unknown }).onModuleDestroy).toBeUndefined();

        const ensureIndexesSpy = vi.spyOn(presenceStore, 'ensureIndexes');
        const clearServerSpy = vi.spyOn(presenceStore, 'clearServer');
        const hubCloseSpy = vi.spyOn(hub, 'close');

        await lifecycle.onModuleInit();
        expect(ensureIndexesSpy).toHaveBeenCalledTimes(1);
        expect(clearServerSpy).not.toHaveBeenCalled();
        expect(hubCloseSpy).not.toHaveBeenCalled();

        await lifecycle.onApplicationShutdown();
        expect(clearServerSpy).toHaveBeenCalledTimes(1);
        expect(hubCloseSpy).toHaveBeenCalledTimes(1);
    });

    it('exports the four tokens plus the lifecycle provider', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
        });
        expect(dynamicModule.module).toBe(RealtimeModule);
        expect(dynamicModule.exports).toEqual(
            expect.arrayContaining([
                SERVER_ID,
                CONNECTION_REGISTRY,
                PRESENCE_STORE,
                CHANGE_STREAM_HUB,
                RealtimeLifecycle,
            ])
        );

        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);
        await (resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub).close();
    });

    it('provides REALTIME_ENABLED, defaulting to true', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);

        expect(resolved.get(REALTIME_ENABLED)).toBe(true);

        await (resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub).close();
    });

    it('provides REALTIME_ENABLED as false when disabled', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
            enabled: false,
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);

        expect(resolved.get(REALTIME_ENABLED)).toBe(false);

        await (resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub).close();
    });
});

describe('RealtimeModule.forRootAsync', () => {
    afterAll(() => stopReplSet());

    it('resolves options via useFactory/inject before building providers', async () => {
        const { db } = await startReplSet();
        const collectionName = uniqueCollection('presence');
        const CONFIG_TOKEN = 'FAKE_CONFIG';

        const dynamicModule = RealtimeModule.forRootAsync({
            inject: [CONFIG_TOKEN],
            useFactory: (config: { db: typeof db }) => ({
                db: config.db,
                presence: { collectionName },
            }),
        });

        // Seed the injected config provider manually, exactly like a real
        // Nest DI container would resolve it from an imported module.
        const providers = [
            { provide: CONFIG_TOKEN, useFactory: () => ({ db }) },
            ...(dynamicModule.providers as unknown as FactoryProvider[]),
        ];
        const resolved = await resolveProviders(providers);

        const presenceStore = resolved.get(PRESENCE_STORE) as PresenceStore;
        expect(presenceStore).toBeInstanceOf(PresenceStore);
        expect(presenceStore.collection.collectionName).toBe(collectionName);

        await (resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub).close();
    });
});
