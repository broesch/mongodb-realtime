export * from './types.js';
export * from './presence-store.js';
export * from './connection-registry.js';
export * from './watch-presence.js';

// Re-exported so this subpath's public signatures are fully self-describing
// for tooling (e.g. api-extractor) that only ever analyzes this entry point
// in isolation — these types are defined elsewhere but appear in signatures
// exported above (ConnectionRegistry, watchPresence, ...).
export type { Logger } from '../common/logger.js';
export type { ReadonlySignal } from '../common/signal.js';
