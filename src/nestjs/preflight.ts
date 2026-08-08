import type { Db } from 'mongodb';
import { consoleLogger, prefixed, type Logger } from '../common/logger.js';
import { enablePrePostImages } from '../core/enable-pre-post-images.js';

const REPLICA_SET_ERROR = [
    'MongoDB is not running as a replica set (or a sharded cluster), so change',
    'streams are unavailable and NO realtime events will be delivered — every',
    'list and detail view will only update on a manual refresh.',
    '',
    'Fix (single-node replica set for local development):',
    '  1. Start mongod with --replSet rs0',
    "  2. mongosh --eval 'rs.initiate()'",
    '  3. Point the driver at mongodb://localhost:27017/?replicaSet=rs0',
    '',
    'Managed deployments (MongoDB Atlas) are replica sets already — a failure',
    'here usually means the connection string points at a plain standalone mongod.',
    '',
    'Or pass { requireChangeStreams: false } to boot without this check (realtime',
    'stays broken; watchers will log a fatal error and stop).',
].join('\n');

export interface PreflightResult {
    /**
     * Whether change streams are usable on this deployment. `false` only ever
     * comes back with `requireChangeStreams: false` — the default throws
     * instead. Feed it to `ChangeStreamEventPusher`'s `enabled` option (or the
     * `REALTIME_ENABLED` provider) so pushers stay inert rather than opening
     * streams that cannot work.
     */
    ok: boolean;
    reason?: 'not-a-replica-set';
}

export interface PreflightOptions {
    /**
     * Collection names to verify (and enable, via `enablePrePostImages`) for
     * `changeStreamPreAndPostImages`. Omit to only run the replica-set check.
     */
    collections?: string[];
    /**
     * When `false`, a non-replica-set deployment logs a warning and returns
     * instead of throwing. Default `true`.
     */
    requireChangeStreams?: boolean;
    logger?: Logger;
}

/**
 * Asserts that MongoDB can actually support the change streams the whole
 * realtime layer is built on: a `hello` probe checks for a replica set (or
 * mongos), and — when `collections` is supplied — `enablePrePostImages` is
 * run and then verified via `listCollections` so a silent `collMod` failure
 * (e.g. unsupported MongoDB version) surfaces at boot instead of at the first
 * hard delete.
 *
 * Ported from fm's `change-stream-preflight.ts`. fm's version reads
 * `CHANGE_STREAMS_ENABLED` off `ConfigService` and wraps this in an
 * `@Injectable` with an `onApplicationBootstrap` hook; this library keeps the
 * assertion as a plain async function so callers can invoke it from whatever
 * bootstrap hook they use (Nest's `onApplicationBootstrap`, a CLI script,
 * etc.) — `requireChangeStreams` replaces the config-service read.
 */
export async function assertChangeStreamPreflight(db: Db, options?: PreflightOptions): Promise<PreflightResult> {
    const log = options?.logger
        ? prefixed(options.logger, 'changeStreamPreflight')
        : prefixed(consoleLogger, 'changeStreamPreflight');
    const requireChangeStreams = options?.requireChangeStreams !== false;

    const info = (await db.admin().command({ hello: 1 })) as {
        setName?: string;
        msg?: string;
    };
    // A replica set member reports `setName`; a mongos reports `msg: isdbgrid`.
    // Change streams work on either; a standalone reports neither.
    const isReplicaSetOrMongos = Boolean(info?.setName) || info?.msg === 'isdbgrid';

    if (!isReplicaSetOrMongos) {
        if (!requireChangeStreams) {
            log.warn(
                'MongoDB is not running as a replica set/mongos and requireChangeStreams is false — ' +
                    'skipping the change-stream preflight. No live entity updates will be delivered.'
            );
            return { ok: false, reason: 'not-a-replica-set' };
        }
        throw new Error(REPLICA_SET_ERROR);
    }

    if (options?.collections && options.collections.length > 0) {
        await enablePrePostImages(db, options.collections, options.logger);
        for (const name of options.collections) {
            const [collInfo] = await db.listCollections({ name }, { nameOnly: false }).toArray();
            const enabled = (collInfo?.options as { changeStreamPreAndPostImages?: { enabled?: boolean } } | undefined)
                ?.changeStreamPreAndPostImages?.enabled;
            if (enabled !== true) {
                throw new Error(
                    [
                        `Change stream pre-images are not enabled on the "${name}" collection after`,
                        'enablePrePostImages ran, so hard deletes there cannot carry the scope/owner',
                        'fields needed to route them — deleted entities would be dropped rather than',
                        'announced.',
                        '',
                        'If the collection already exists, enable pre-images in place:',
                        `  mongosh --eval 'db.runCommand({ collMod: "${name}", changeStreamPreAndPostImages: { enabled: true } })'`,
                        '',
                        'If you are provisioning a FRESH database, create it with pre-images enabled',
                        'instead, before first run — an ODM that auto-creates the collection would',
                        'otherwise create it without them and the next restart would hit this again:',
                        `  mongosh --eval 'db.createCollection("${name}", { changeStreamPreAndPostImages: { enabled: true } })'`,
                        '',
                        'Or pass { requireChangeStreams: false } to boot without realtime.',
                    ].join('\n')
                );
            }
        }
    }

    log.log('Change stream preflight passed');
    return { ok: true };
}
