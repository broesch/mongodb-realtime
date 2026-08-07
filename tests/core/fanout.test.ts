import { describe, it, expect, vi } from 'vitest';
import { Fanout } from '../../src/core/fanout.js';
import { Logger } from '../../src/common/logger.js';

describe('Fanout', () => {
    it('two consumers both receive every pushed item', async () => {
        const fanout = new Fanout<number>();
        const gen1 = fanout.consumer();
        const gen2 = fanout.consumer();

        fanout.push(1);
        fanout.push(2);
        fanout.push(3);
        fanout.close();

        const results1: number[] = [];
        for await (const item of gen1) results1.push(item);

        const results2: number[] = [];
        for await (const item of gen2) results2.push(item);

        expect(results1).toEqual([1, 2, 3]);
        expect(results2).toEqual([1, 2, 3]);
    });

    it('items pushed while a consumer is not awaiting are buffered and delivered in order', async () => {
        const fanout = new Fanout<number>();
        const gen = fanout.consumer();

        // Push items before consuming
        fanout.push(1);
        fanout.push(2);
        fanout.push(3);

        const results: number[] = [];
        fanout.close();
        for await (const item of gen) results.push(item);

        expect(results).toEqual([1, 2, 3]);
    });

    it('pushing beyond maxQueue drops the overflow and logs one warning via injected spy logger', async () => {
        const warnSpy = vi.fn();
        const mockLogger: Logger = {
            debug: () => undefined,
            log: () => undefined,
            warn: warnSpy,
            error: () => undefined,
        };

        const fanout = new Fanout<number>({ maxQueue: 5, label: 'test', logger: mockLogger });
        const gen = fanout.consumer();

        // Push 5 items — should all fit
        fanout.push(1);
        fanout.push(2);
        fanout.push(3);
        fanout.push(4);
        fanout.push(5);

        // Push 1 more — should drop and log warning once
        fanout.push(6);

        // Push more — should drop but log only additional warnings
        fanout.push(7);
        fanout.push(8);

        // Verify warn was called 3 times (one per overflow item)
        // Note: the warn call includes the message with fanout label
        expect(warnSpy).toHaveBeenCalledTimes(3);

        // Verify the first 5 items are still in the consumer queue
        fanout.close();
        const results: number[] = [];
        for await (const item of gen) results.push(item);

        expect(results).toEqual([1, 2, 3, 4, 5]);
    });

    it('close() terminates all consumer generators (for-await loops end)', async () => {
        const fanout = new Fanout<string>();
        const gen1 = fanout.consumer();
        const gen2 = fanout.consumer();

        // Close immediately with no items pushed
        fanout.close();

        const results1: string[] = [];
        for await (const item of gen1) results1.push(item);

        const results2: string[] = [];
        for await (const item of gen2) results2.push(item);

        expect(results1).toEqual([]);
        expect(results2).toEqual([]);
    });

    it('a consumer that breaks out of its loop is removed from the set (size drops)', async () => {
        const fanout = new Fanout<number>();

        expect(fanout.size).toBe(0);

        const gen = fanout.consumer();
        expect(fanout.size).toBe(1);

        // Push one item so the consumer has something to break on
        fanout.push(42);

        // Break after first item — triggers the generator's finally block
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const item of gen) {
            break;
        }

        // After break the finally block removes the consumer
        expect(fanout.size).toBe(0);
    });

    it('close() resolves a consumer that is already awaiting the next item', async () => {
        const fanout = new Fanout<number>();
        const consumer = fanout.consumer();
        const pending = consumer.next(); // parks the generator at its internal await
        await new Promise(resolve => setImmediate(resolve)); // let it actually reach the await
        fanout.close();
        const result = await pending;
        expect(result).toEqual({ value: undefined, done: true });
        expect(fanout.size).toBe(0); // finally block deregistered it
    });
});
