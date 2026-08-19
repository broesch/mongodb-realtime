export * from './change-stream-event-pusher.js';
export * from './nest-logger.js';
export * from './preflight.js';
export * from './realtime-module.js';
export * from './socket-server-handle.js';

// Re-exported so this subpath's public signatures are fully self-describing
// for tooling (e.g. api-extractor) that only ever analyzes this entry point
// in isolation — these types are defined elsewhere but appear in signatures
// exported above (ChangeStreamEventPusher, RealtimeModule, ...).
export type { ValueSignal, Signal, ReadonlySignal } from '../common/signal.js';
export type { Logger } from '../common/logger.js';
export type { ResumeOptions } from '../core/change-stream-subscription.js';
export type { PresenceStoreOptions, PresenceDoc } from '../presence/types.js';
export type { ChangeStreamHubOptions } from '../core/change-stream-hub.js';
export type { PresenceStore } from '../presence/presence-store.js';
export type { ChangeStreamHub } from '../core/change-stream-hub.js';
export type { DbResolver } from '../core/db-resolver.js';
