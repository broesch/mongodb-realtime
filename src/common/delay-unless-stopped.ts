/**
 * Wait `ms`, or resolve early with `'stopped'` if `stop` settles first.
 *
 * Used by every change-stream reconnect backoff. Two properties matter and are
 * easy to lose if this is reimplemented inline:
 *   - the timer is `unref()`'d, so a pending backoff can never keep the process
 *     alive (a 30s backoff would otherwise hold the event loop open through an
 *     e2e teardown);
 *   - it is cancellable, so shutdown does not have to wait out the full delay.
 *     Without this, raising the backoff ceiling turns a bounded shutdown into a
 *     multi-second one.
 *
 * The timer is cleared on early return so it can't fire (and log) after a real
 * shutdown has already raced it out.
 */
export function delayUnlessStopped(ms: number, stop: Promise<unknown>): Promise<'elapsed' | 'stopped'> {
    return new Promise(resolve => {
        let settled = false;
        const timer = setTimeout(() => {
            if (!settled) {
                settled = true;
                resolve('elapsed');
            }
        }, ms);
        if (typeof timer.unref === 'function') timer.unref();
        void stop.then(() => {
            if (!settled) {
                settled = true;
                clearTimeout(timer);
                resolve('stopped');
            }
        });
    });
}
