import { ChangeStreamEventPusher } from '../../src/nestjs/change-stream-event-pusher.js';

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
