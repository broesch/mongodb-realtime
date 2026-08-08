import { vi } from 'vitest';
import { ChangeStreamHub } from '../../src/core/change-stream-hub.js';
import { ConnectionRegistry } from '../../src/presence/connection-registry.js';
import { PresenceStore } from '../../src/presence/presence-store.js';
import {
    CHANGE_STREAM_HUB,
    CONNECTION_REGISTRY,
    PRESENCE_STORE,
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

    it('the lifecycle provider runs ensureIndexes on init and clearServer + hub.close on shutdown', async () => {
        const { db } = await startReplSet();
        const dynamicModule = RealtimeModule.forRoot({
            db,
            presence: { collectionName: uniqueCollection('presence') },
        });
        const resolved = await resolveProviders(dynamicModule.providers as unknown as FactoryProvider[]);

        const presenceStore = resolved.get(PRESENCE_STORE) as PresenceStore;
        const hub = resolved.get(CHANGE_STREAM_HUB) as ChangeStreamHub;
        const lifecycle = resolved.get(RealtimeLifecycle) as RealtimeLifecycle;

        const ensureIndexesSpy = vi.spyOn(presenceStore, 'ensureIndexes');
        const clearServerSpy = vi.spyOn(presenceStore, 'clearServer');
        const hubCloseSpy = vi.spyOn(hub, 'close');

        await lifecycle.onModuleInit();
        expect(ensureIndexesSpy).toHaveBeenCalledTimes(1);
        expect(clearServerSpy).not.toHaveBeenCalled();
        expect(hubCloseSpy).not.toHaveBeenCalled();

        await lifecycle.onModuleDestroy();
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
