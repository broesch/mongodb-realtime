import { Logger as NestLogger } from '@nestjs/common';
import type { Logger } from '../common/logger.js';

/**
 * The subset of a NestJS logger this adapter drives. Declared structurally so
 * a test (or a consumer with a custom logger) can pass a stand-in without
 * constructing a real `NestLogger`.
 */
export interface NestLoggerLike {
    debug(message: string, ...rest: unknown[]): void;
    log(message: string, ...rest: unknown[]): void;
    warn(message: string, ...rest: unknown[]): void;
    error(message: string, ...rest: unknown[]): void;
}

/**
 * Adapts a NestJS logger to this library's structural `Logger`, so
 * change-stream and fan-out logs land in the same stream and format as the
 * rest of the host application instead of going to bare console.
 *
 * Both consumers wrote this by hand before it lived here.
 */
export function nestLogger(context: string, impl?: NestLoggerLike): Logger {
    const target: NestLoggerLike = impl ?? new NestLogger(context);
    return {
        debug: message => target.debug(message),
        log: message => target.log(message),
        warn: message => target.warn(message),
        error: (message, stack) => (stack === undefined ? target.error(message) : target.error(message, stack)),
    };
}
