import { delayUnlessStopped } from '../../src/common/delay-unless-stopped.js';

describe('delayUnlessStopped', () => {
    it('resolves "elapsed" after the delay', async () => {
        await expect(delayUnlessStopped(10, new Promise(() => {}))).resolves.toBe('elapsed');
    });

    it('resolves "stopped" immediately when stop settles first', async () => {
        const start = Date.now();
        await expect(delayUnlessStopped(5_000, Promise.resolve())).resolves.toBe('stopped');
        expect(Date.now() - start).toBeLessThan(1_000);
    });
});
