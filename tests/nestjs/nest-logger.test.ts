import { nestLogger } from '../../src/nestjs/nest-logger.js';

describe('nestLogger', () => {
    it('forwards each level to the injected Nest logger', () => {
        const calls: Array<[string, unknown[]]> = [];
        const fake = {
            debug: (...args: unknown[]) => calls.push(['debug', args]),
            log: (...args: unknown[]) => calls.push(['log', args]),
            warn: (...args: unknown[]) => calls.push(['warn', args]),
            error: (...args: unknown[]) => calls.push(['error', args]),
        };

        const logger = nestLogger('Ctx', fake);
        logger.debug('d');
        logger.log('l');
        logger.warn('w');
        logger.error('e', 'stack-trace');

        expect(calls).toEqual([
            ['debug', ['d']],
            ['log', ['l']],
            ['warn', ['w']],
            ['error', ['e', 'stack-trace']],
        ]);
    });
});
