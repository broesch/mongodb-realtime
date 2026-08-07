export interface ReadonlySignal<T> {
    getValue(): T;
    /** Emits the current value synchronously on subscribe (BehaviorSubject-compatible). */
    subscribe(listener: (value: T) => void): { unsubscribe(): void };
}

export interface Signal<T> extends ReadonlySignal<T> {
    next(value: T): void;
}

/** Minimal BehaviorSubject replacement so the library has no rxjs dependency. */
export class ValueSignal<T> implements Signal<T> {
    private readonly listeners = new Set<(value: T) => void>();

    constructor(private value: T) {}

    getValue(): T {
        return this.value;
    }

    next(value: T): void {
        this.value = value;
        // Copy so a listener unsubscribing (or subscribing) mid-notification is safe.
        for (const listener of [...this.listeners]) {
            if (this.listeners.has(listener)) listener(value);
        }
    }

    subscribe(listener: (value: T) => void): { unsubscribe(): void } {
        this.listeners.add(listener);
        listener(this.value);
        return { unsubscribe: () => this.listeners.delete(listener) };
    }
}

/** Subscribe skipping the initial synchronous emission — replaces rxjs `.pipe(skip(1)).subscribe(fn)`. */
export function subscribeToChanges<T>(
    signal: ReadonlySignal<T>,
    listener: (value: T) => void
): { unsubscribe(): void } {
    let first = true;
    return signal.subscribe(value => {
        if (first) {
            first = false;
            return;
        }
        listener(value);
    });
}
