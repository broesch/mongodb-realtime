import { ValueSignal, subscribeToChanges } from '../../src/common/signal.js';

describe('ValueSignal', () => {
    it('holds and returns the current value', () => {
        const s = new ValueSignal(1);
        expect(s.getValue()).toBe(1);
        s.next(2);
        expect(s.getValue()).toBe(2);
    });

    it('subscribe emits the current value synchronously, then updates', () => {
        const s = new ValueSignal('a');
        const seen: string[] = [];
        const sub = s.subscribe(v => seen.push(v));
        s.next('b');
        expect(seen).toEqual(['a', 'b']);
        sub.unsubscribe();
        s.next('c');
        expect(seen).toEqual(['a', 'b']);
    });

    it('subscribeToChanges skips the initial emission', () => {
        const s = new ValueSignal(0);
        const seen: number[] = [];
        subscribeToChanges(s, v => seen.push(v));
        expect(seen).toEqual([]);
        s.next(1);
        expect(seen).toEqual([1]);
    });

    it('a listener that unsubscribes during notification does not break other listeners', () => {
        const s = new ValueSignal(0);
        const seen: number[] = [];
        let sub: { unsubscribe(): void } | undefined;
        // eslint-disable-next-line prefer-const
        sub = s.subscribe(() => sub?.unsubscribe()); // initial sync emission: sub still undefined — no-op
        s.subscribe(v => seen.push(v));
        s.next(1); // sub is set now; unsubscribing mid-notification must not affect the second listener
        expect(seen).toEqual([0, 1]);
        s.next(2); // first listener is gone; second still receives
        expect(seen).toEqual([0, 1, 2]);
    });
});
