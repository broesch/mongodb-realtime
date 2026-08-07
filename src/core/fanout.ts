import { Logger, consoleLogger, prefixed } from '../common/logger.js';

interface Consumer<T> {
    queue: T[];
    resolve?: (r: IteratorResult<T>) => void;
    closed: boolean;
}

/**
 * Single-producer / multi-consumer async fan-out queue.
 *
 * The per-tenant change-stream hub pushes one DB-level change stream event to
 * N per-collection consumers via this class. Each consumer gets its own
 * independent async generator; items pushed while the consumer is busy are
 * buffered up to `maxQueue` (drops with a warning when full). Calling
 * `close()` terminates all consumer generators cleanly.
 *
 * A consumer that `break`s out of its `for await` loop is automatically
 * removed from the fan-out set via the generator's `finally` block.
 */
export class Fanout<T> {
    private readonly consumers = new Set<Consumer<T>>();
    private readonly log: Logger;
    private readonly maxQueue: number;
    private readonly label: string;

    constructor(options?: { maxQueue?: number; label?: string; logger?: Logger }) {
        this.maxQueue = options?.maxQueue ?? 10_000;
        this.label = options?.label ?? '';
        const baseLogger = options?.logger ?? consoleLogger;
        this.log = prefixed(baseLogger, `ChangeStreamFanout${this.label ? `:${this.label}` : ''}`);
    }

    push(item: T): void {
        for (const c of this.consumers) {
            if (c.resolve) {
                const r = c.resolve;
                c.resolve = undefined;
                r({ value: item, done: false });
            } else if (c.queue.length < this.maxQueue) {
                c.queue.push(item);
            } else {
                this.log.warn(`fanout ${this.label} consumer queue full (${this.maxQueue}) — dropping change`);
            }
        }
    }

    close(): void {
        for (const c of this.consumers) {
            c.closed = true;
            c.resolve?.({ value: undefined as never, done: true });
        }
    }

    get size(): number {
        return this.consumers.size;
    }

    consumer(): AsyncGenerator<T> {
        const c: Consumer<T> = { queue: [], closed: false };
        this.consumers.add(c);
        const consumers = this.consumers;
        return (async function* () {
            try {
                while (true) {
                    if (c.queue.length) {
                        yield c.queue.shift() as T;
                        continue;
                    }
                    if (c.closed) return;
                    const r = await new Promise<IteratorResult<T>>(res => {
                        c.resolve = res;
                    });
                    if (r.done) return;
                    yield r.value;
                }
            } finally {
                consumers.delete(c);
            }
        })();
    }
}
