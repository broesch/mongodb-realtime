# mongodb-realtime

Realtime infrastructure on MongoDB change streams — resumable subscriptions, per-key watch hubs, presence, socket.io fan-out. No Redis.

Multi-node realtime today needs a pub/sub bus so every server process sees every event, not just the one that handled the write. This library uses MongoDB's own change streams as that bus — no Redis, no separate message broker — plus the supporting pieces every app built on top of it ends up needing: a resumable subscription that survives transient errors and reopens on a scope change, a per-key hub that demultiplexes many collection watchers onto one DB-level cursor, a TTL'd presence store with cross-node online/offline detection, and a handful of socket.io fan-out loops with the permission-filtering and PII-redaction hooks real apps need.

## Features

- **Resumable subscriptions** — `ChangeStreamSubscription` reopens on transient errors with capped exponential backoff, resumes from `startAfter`/`startAtOperationTime`, reclassifies fatal vs. unresumable errors, and parks (no open cursor) while its scope is empty
- **Cursor reduction** — `ChangeStreamHub` opens one DB-level `db.watch()` per key and demuxes to per-collection consumers, so cursor count drops from O(watchers) to O(keys); optional connection-driven parking with a reconnect grace window
- **Canonical change events** — `changeDocToEvent` turns a raw change document into a `{ type: 'changed' | 'removed', scopeId, ... }` event under configurable field-name conventions (`scopeField`/`softDeleteField`/`ownerField`), recovering `scopeId`/`ownerId` for hard deletes from the pre-image
- **Multi-tenant composition** — `watchCollection` + `mergeAsyncGenerators` + per-source `tenant` tagging fan N tenant DBs' watches into one merged, backpressure-bounded stream
- **Presence** — `PresenceStore` (TTL'd `onlineUsers` collection, atomic check-in with DB-arbitrated race resolution), `ConnectionRegistry` (per-node connected-scope/-key signals), `watchPresence` (cross-node 0→1/1→0 online/offline transition detection)
- **Socket.io fan-out** — `pushScoped`, `pushOwnerScoped`, `pushScopedFiltered` (per-recipient permission filtering + two-variant PII rendering); shared room-naming helpers so join and emit sites can't disagree
- **NestJS binding** — `RealtimeModule.forRoot/forRootAsync` wires the hub/presence-store/connection-registry behind injection tokens; `ChangeStreamEventPusher` is a lifecycle base with a crash latch so a pusher that dies during startup can't hang shutdown; `assertChangeStreamPreflight` fails boot fast with an actionable message on a non-replica-set deployment
- **Zero runtime dependencies** — `mongodb` is a wide peer (`^5.7 || ^6 || ^7`); `socket.io` and `@nestjs/common` are optional peers, imported type-only
- **Framework-agnostic core** — the engine (`ChangeStreamSubscription`, `ChangeStreamHub`, `watchCollection`) has no socket.io or NestJS dependency; those are opt-in subpaths

## Requirements

- **A MongoDB replica set (or sharded cluster).** Change streams don't exist on a standalone `mongod` — this library is built on them throughout (subscriptions, the hub, presence, preflight). For local development, a single-node replica set is enough:
  ```bash
  mongod --replSet rs0
  mongosh --eval 'rs.initiate()'
  ```
  Then connect with `?replicaSet=rs0` in the connection string. Managed deployments (MongoDB Atlas) are replica sets already.
- **Node.js >= 20**
- **For NestJS apps:** call `app.enableShutdownHooks()` — without it, Nest never invokes `onApplicationShutdown`, so neither `RealtimeModule`'s hub/presence teardown nor any `ChangeStreamEventPusher` subclass's shutdown drain ever runs. Import `RealtimeModule` from the **root** module (not lazily from a feature module): within a shutdown phase Nest runs hooks in reverse instantiation order, so `RealtimeModule`'s own teardown must run *last*, after every gateway built on it has already been signalled to stop.

## Installation

Not published to npm yet — install as a git dependency:

```bash
pnpm add github:broesch/mongodb-realtime
```

`mongodb` is a required peer; `socket.io` and `@nestjs/common` are optional peers — install whichever subpaths you use:

```bash
pnpm add mongodb
pnpm add socket.io          # only if you use mongodb-realtime/socketio
pnpm add @nestjs/common     # only if you use mongodb-realtime/nestjs
```

## Quick start

### Single-DB app: hub + `watchCollection` + `pushScoped`

One `ChangeStreamHub` per process; every collection subscribed under the same `key` shares its one DB-level cursor.

```ts
import { createServer } from 'node:http';
import { MongoClient } from 'mongodb';
import { Server } from 'socket.io';
import { ChangeStreamHub, ValueSignal, watchCollection } from 'mongodb-realtime';
import { pushScoped } from 'mongodb-realtime/socketio';

const client = new MongoClient('mongodb://localhost:27017/?replicaSet=rs0');
await client.connect();
const db = client.db('myapp');

const httpServer = createServer();
const io = new Server(httpServer);

const hub = new ChangeStreamHub();
const shutdown = new ValueSignal(false);

const watcher = watchCollection({
    source: { hub, key: 'default', db, collection: 'tasks' },
    shutdown,
});

void pushScoped(io, watcher, { changed: 'task:changed', removed: 'task:removed' }, { shutdown });

httpServer.listen(3000);

process.on('SIGTERM', () => {
    void (async () => {
        shutdown.next(true);
        await hub.close();
        httpServer.close();
    })();
});
```

Clients join `scopeRoom(orgId)` (from `mongodb-realtime/socketio`) on connect; `pushScoped` emits `task:changed`/`task:removed` to that same room, so join and emit sites can never disagree about the room name.

### Multi-tenant app: N tagged `watchCollection`s + `mergeAsyncGenerators`

Each tenant gets its own `watchCollection` generator tagged with that tenant's identity; `mergeAsyncGenerators` fans them into one stream with bounded per-source lookahead.

```ts
import type { Db } from 'mongodb';
import { ValueSignal, collectionWatchFactory, mergeAsyncGenerators, watchCollection } from 'mongodb-realtime';

const shutdown = new ValueSignal(false);

const tenantDbs: { name: string; db: Db }[] = [
    { name: 'tenant-a', db: dbA },
    { name: 'tenant-b', db: dbB },
];

const watchers = tenantDbs.map(tenant =>
    watchCollection({
        source: {
            watch: collectionWatchFactory(tenant.db.collection('tasks')),
            db: tenant.db,
            collection: 'tasks',
        },
        shutdown,
        tenant: tenant.name,
    })
);

for await (const event of mergeAsyncGenerators(watchers)) {
    // event.tenant identifies which tenant DB the change came from.
    console.log(event.tenant, event.type);
}
```

`collectionWatchFactory` builds a correct `watch` factory (the four relevant operation types, `fullDocument: 'updateLookup'`, `fullDocumentBeforeChange: 'whenAvailable'`, resume options spread in) from a plain driver `Collection`. Supply your own `watch` function instead of `collectionWatchFactory(...)` if you need a custom aggregation pipeline — it must do the same three things.

### Presence: `PresenceStore` + `ConnectionRegistry` + `watchPresence`

```ts
import type { Db } from 'mongodb';
import type { Server, Socket } from 'socket.io';
import { ValueSignal } from 'mongodb-realtime';
import { ConnectionRegistry, PresenceStore, watchPresence } from 'mongodb-realtime/presence';
import { scopeRoom } from 'mongodb-realtime/socketio';

async function wirePresence(db: Db, io: Server): Promise<void> {
    const shutdown = new ValueSignal(false);
    const store = new PresenceStore({ db, serverId: process.env.HOSTNAME ?? 'server-1' });
    await store.ensureIndexes();

    const registry = new ConnectionRegistry();

    io.on('connection', (socket: Socket) => {
        const userId = socket.handshake.auth.userId as string;
        const scopeId = socket.handshake.auth.orgId as string;

        registry.add({ clientId: socket.id, userId, scopeId });
        void store.checkIn({ userId, clientId: socket.id, scopeId });
        void socket.join(scopeRoom(scopeId));

        socket.on('disconnect', () => {
            registry.remove(socket.id);
            void store.checkOut(userId, socket.id);
        });
    });

    for await (const event of watchPresence({ store, shutdown })) {
        io.to(scopeRoom(event.scopeId)).emit('presence:changed', event);
    }
}
```

`registry.connectedScopes`/`registry.connectedKeys` are `ReadonlySignal<string[]>`s you can wire straight into a `ChangeStreamHub`'s `connectedKeys` option to park a scope's/tenant's watch when nobody on this node is connected to it — see the NestJS example below, where `RealtimeModule` does exactly this by default.

### NestJS: `RealtimeModule.forRoot` + a `ChangeStreamEventPusher` subclass

```ts
import { Inject, Injectable, Module } from '@nestjs/common';
import type { Db } from 'mongodb';
import type { Server } from 'socket.io';
import { watchCollection, type ChangeStreamHub } from 'mongodb-realtime';
import { pushScoped } from 'mongodb-realtime/socketio';
import { CHANGE_STREAM_HUB, ChangeStreamEventPusher, RealtimeModule } from 'mongodb-realtime/nestjs';

@Injectable()
export class TaskPusher extends ChangeStreamEventPusher {
    constructor(
        @Inject(CHANGE_STREAM_HUB) private readonly hub: ChangeStreamHub,
        @Inject('DB') private readonly db: Db,
        @Inject('SOCKET_SERVER') private readonly io: Server
    ) {
        super();
    }

    protected async pushChangeEvents(): Promise<void> {
        try {
            const watcher = watchCollection({
                source: { hub: this.hub, key: 'default', db: this.db, collection: 'tasks' },
                shutdown: this.shutdown, // ValueSignal<boolean> owned by the base class
            });
            await pushScoped(
                this.io,
                watcher,
                { changed: 'task:changed', removed: 'task:removed' },
                { shutdown: this.shutdown }
            );
        } finally {
            this.loopEnded(); // resolves the shutdown latch onApplicationShutdown awaits
        }
    }
}

@Module({
    imports: [RealtimeModule.forRoot({ db })], // db: your connected mongodb.Db instance
    providers: [TaskPusher],
})
export class AppModule {}
```

`RealtimeModule.forRoot` provides `SERVER_ID`, `CONNECTION_REGISTRY`, `PRESENCE_STORE`, `CHANGE_STREAM_HUB` behind string tokens, plus a lifecycle service that calls `presenceStore.ensureIndexes()` on init and `presenceStore.clearServer()` + `hub.close()` on `onApplicationShutdown`. By default (`wireHubParking: true`) the hub's `connectedKeys` is wired to the registry's `connectedKeys`, so a tenant's watch parks once nobody on this node is connected to it — wire `ConnectionRegistry.add`/`.remove()` into your gateway's connect/disconnect handlers to drive it. `ChangeStreamEventPusher` subclasses call `this.loopEnded()` in their loop's `finally`; a `pushChangeEvents()` that rejects before reaching that `finally` (e.g. the watcher throws during setup) is still caught and resolves the same latch, so `onApplicationShutdown` can never hang on a pusher that crashed during startup.

Run `assertChangeStreamPreflight(db, { collections: [...] })` at boot (e.g. in `onApplicationBootstrap`) to fail fast with an actionable message instead of silently degrading on a non-replica-set deployment.

## Change event conventions

`changeDocToEvent` (and everything built on it — `watchCollection`, the socket.io fan-out loops) reads three configurable fields off your documents:

| Convention | Option | Default | Meaning |
|---|---|---|---|
| Scope field | `scopeField` | `organizationId` | Tenant/organization the document belongs to; becomes `event.scopeId` |
| Soft-delete field | `softDeleteField` | `deleted` | Boolean field; `true` turns an `update` into a `removed` event |
| Owner field | `ownerField` | `userId` | User the document belongs to; becomes `event.ownerId` (used by `pushOwnerScoped`) |

Override any subset via `conventions` on `watchCollection` (or call `changeDocToEvent` directly):

```ts
watchCollection({
    source: { watch: collectionWatchFactory(coll), db, collection: 'bookings' },
    shutdown,
    conventions: { scopeField: 'tenantId', softDeleteField: 'archived', ownerField: 'ownerId' },
});
```

A hard delete recovers `scopeId`/`ownerId` from the pre-image (`fullDocumentBeforeChange`) — this is why every entry point (`watchCollection`, `PresenceStore.ensureIndexes`, `assertChangeStreamPreflight`) calls `enablePrePostImages` for you; you don't need to call it yourself.

## What stays in your app

This library is mechanism, not policy. It deliberately does **not** decide:

- **Event names and DTOs** — the `events: { changed, removed? }` strings and the shape of `event.data` are yours
- **Permission logic** — the body of `canReceive` in `pushScopedFiltered`; the library only calls it per recipient and skips whoever it rejects
- **PII redaction rules** — the body of `variants.render`/`variants.pick`; the library only threads the two pre-rendered payloads to the right sockets
- **Room-join policy** — when/whether a socket joins a scope or user room (typically at connection time, after your own auth); the library only supplies the room-naming helpers (`userRoom`/`scopeRoom`/`namespacedRoom`) so join and emit sites can't disagree
- **Tenancy resolution** — how you decide which DB/tenant a request or socket belongs to (JWT claim, CLS, per-request header, …); the library treats `tenant` as an opaque generic value it only tags events with and threads to `tenantNamespace`/`runInContext`/`tenantOf`
- **Auth** — WebSocket auth strategies/guards are entirely your gateway's concern; sockets arrive at every hook already authenticated (or not, by your own policy)

## Notes

- **Catch your push-loop promises.** `pushScoped`/`pushOwnerScoped`/`pushScopedFiltered` return a `Promise<void>` that only resolves when the watcher completes — normally only on shutdown. Outside `ChangeStreamEventPusher` (which already has a crash latch via its `.catch` in `onApplicationBootstrap`), an unawaited call that rejects becomes an unhandled rejection; attach a `.catch` yourself if you're not going through the NestJS base class.
- **No socket.io adapter, by design.** This library does not use (and does not require) `@socket.io/redis-adapter` or any other adapter — `server.to(room).emit(...)` inside `pushScoped`/`pushOwnerScoped`/`pushScopedFiltered` only reaches sockets connected to *that process*, and change streams are what make every process's own emit collectively cover every connected client. If you *do* run a socket.io adapter for some other reason, be aware that `RemoteSocket.data` (read by `pushScopedFiltered`'s `canReceive`/`tenantOf`/`variants.pick`) is only reliably populated for sockets local to the process under most adapters — this library's multi-node story is adapter-less by design specifically to avoid that gap.

## License

MIT
