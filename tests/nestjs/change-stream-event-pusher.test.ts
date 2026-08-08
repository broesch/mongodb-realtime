import { ChangeStreamEventPusher } from '../../src/nestjs/change-stream-event-pusher.js';
import type { Logger } from '../../src/common/logger.js';
import { startReplSet, stopReplSet, uniqueCollection } from '../helpers/replset.js';

interface LogCall {
    level: 'debug' | 'log' | 'warn' | 'error';
    message: string;
}

function mockLogger(): { logger: Logger; calls: LogCall[] } {
    const calls: LogCall[] = [];
    return {
        calls,
        logger: {
            debug: message => calls.push({ level: 'debug', message }),
            log: message => calls.push({ level: 'log', message }),
            warn: message => calls.push({ level: 'warn', message }),
            error: message => calls.push({ level: 'error', message }),
        },
    };
}

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

class OrderlyPusher extends ChangeStreamEventPusher {
    ran = false;

    protected async pushChangeEvents(): Promise<void> {
        this.ran = true;
        try {
            // Simulate a loop that waits on the shutdown signal before exiting,
            // like pushScoped's `for await (const event of watcher)` does once
            // the watcher itself observes `shutdown`.
            await new Promise<void>(resolve => {
                const check = (): void => {
                    if (this.shutdown.getValue()) {
                        resolve();
                        return;
                    }
                    setTimeout(check, 5);
                };
                check();
            });
        } finally {
            this.loopEnded();
        }
    }
}

class CrashingPusher extends ChangeStreamEventPusher {
    constructor(private readonly error: Error) {
        super();
    }

    protected pushChangeEvents(): Promise<void> {
        return Promise.reject(this.error);
    }
}

describe('ChangeStreamEventPusher', () => {
    it('onApplicationBootstrap runs pushChangeEvents', () => {
        const pusher = new OrderlyPusher();
        pusher.onApplicationBootstrap();
        expect(pusher.ran).toBe(true);
    });

    it('a rejecting pushChangeEvents logs the error and lets onApplicationShutdown resolve promptly (crash latch)', async () => {
        const errors: Array<{ message: string; stack?: string }> = [];
        const pusher = new CrashingPusher(new Error('boom'));
        // @ts-expect-error -- reach into the protected logger to assert on it
        pusher.logger.error = (message: string, stack?: string) => errors.push({ message, stack });

        pusher.onApplicationBootstrap();

        // Give the fire-and-forget rejection a turn to settle before shutdown.
        await new Promise(resolve => setTimeout(resolve, 10));

        await expect(pusher.onApplicationShutdown()).resolves.toBeUndefined();
        expect(errors).toHaveLength(1);
        expect(errors[0]?.message).toContain('boom');
    });

    it('orderly path: onApplicationShutdown signals shutdown and awaits until loopEnded() is called', async () => {
        const pusher = new OrderlyPusher();
        pusher.onApplicationBootstrap();

        // @ts-expect-error -- reach into the protected shutdown signal to assert on it
        expect(pusher.shutdown.getValue()).toBe(false);

        const shutdownPromise = pusher.onApplicationShutdown();

        // @ts-expect-error -- protected, but shutdown must have been signalled synchronously
        expect(pusher.shutdown.getValue()).toBe(true);

        await expect(shutdownPromise).resolves.toBeUndefined();
    });

    it('onApplicationShutdown does not resolve before loopEnded() is called', async () => {
        const pusher = new OrderlyPusher();
        pusher.onApplicationBootstrap();

        let resolved = false;
        const shutdownPromise = pusher.onApplicationShutdown().then(() => {
            resolved = true;
        });

        // OrderlyPusher's loop polls every 5ms and only resolves loopEnded()
        // once it observes `shutdown`; before that tick, shutdown must still
        // be pending.
        await new Promise(resolve => setTimeout(resolve, 0));
        expect(resolved).toBe(false);

        await shutdownPromise;
        expect(resolved).toBe(true);
    });
});

describe('ChangeStreamEventPusher — enabled gate and latching', () => {
    it('never invokes pushChangeEvents when constructed disabled, and shutdown still returns', async () => {
        let invoked = false;
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                invoked = true;
            }
        }
        const pusher = new Pusher({ enabled: false });

        pusher.onApplicationBootstrap();
        await expect(pusher.onApplicationShutdown()).resolves.toBeUndefined();
        expect(invoked).toBe(false);
    });

    it('shutdown returns even when the loop resolves without calling loopEnded', async () => {
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                // Deliberately does NOT call this.loopEnded().
            }
        }
        const pusher = new Pusher();

        pusher.onApplicationBootstrap();
        await expect(pusher.onApplicationShutdown()).resolves.toBeUndefined();
    });

    it('logs an error when the loop ends while the app is still running', async () => {
        const { logger, calls } = mockLogger();
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                this.markWatching();
            }
        }
        const pusher = new Pusher({ logger });

        pusher.onApplicationBootstrap();
        await delay(50);

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown was requested'))).toBe(true);
    });

    it('does not log that error when the loop ends during shutdown', async () => {
        const { logger, calls } = mockLogger();
        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                this.markWatching();
                await new Promise<void>(resolve => {
                    const check = (): void => {
                        if (this.shutdown.getValue()) resolve();
                        else setTimeout(check, 5);
                    };
                    check();
                });
            }
        }
        const pusher = new Pusher({ logger });

        pusher.onApplicationBootstrap();
        await delay(20);
        await pusher.onApplicationShutdown();

        expect(calls.some(c => c.level === 'error' && c.message.includes('before shutdown was requested'))).toBe(false);
    });
});

describe('ChangeStreamEventPusher — watchOne', () => {
    afterAll(() => stopReplSet());

    it('watchOne opens a stream, delivers each change to the handler, and stops on shutdown', async () => {
        const { db } = await startReplSet();
        const name = uniqueCollection('pusher-watch-one');
        const coll = db.collection(name);
        const seen: string[] = [];

        class Pusher extends ChangeStreamEventPusher {
            protected async pushChangeEvents(): Promise<void> {
                await this.watchOne({
                    collectionName: name,
                    watch: resumeOptions => coll.watch([], { fullDocument: 'updateLookup', ...resumeOptions }),
                    handler: change => {
                        seen.push(change.operationType);
                    },
                });
            }
        }
        const pusher = new Pusher();

        pusher.onApplicationBootstrap();
        await delay(400);
        await coll.insertOne({ v: 1 });
        await delay(400);

        await pusher.onApplicationShutdown();
        expect(seen).toContain('insert');
    });
});
