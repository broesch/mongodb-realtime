/** Structural logger contract (same shape as mongodb-task-queue's). */
export interface Logger {
    debug(message: string): void;
    log(message: string): void;
    warn(message: string): void;
    error(message: string, stack?: string): void;
}

export const consoleLogger: Logger = {
    debug: () => undefined, // debug is opt-in; default logger stays quiet
    log: message => console.log(message),
    warn: message => console.warn(message),
    error: (message, stack) => console.error(stack ? `${message}\n${stack}` : message),
};

export const noopLogger: Logger = {
    debug: () => undefined,
    log: () => undefined,
    warn: () => undefined,
    error: () => undefined,
};

/** Wrap a logger so every message carries a `[prefix]` tag (replaces per-class nest Logger names). */
export function prefixed(base: Logger, prefix: string): Logger {
    const tag = (message: string) => `[${prefix}] ${message}`;
    return {
        debug: message => base.debug(tag(message)),
        log: message => base.log(tag(message)),
        warn: message => base.warn(tag(message)),
        error: (message, stack) => base.error(tag(message), stack),
    };
}
