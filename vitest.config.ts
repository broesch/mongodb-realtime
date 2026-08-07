import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        globals: true,
        testTimeout: 60_000,
        hookTimeout: 120_000, // replica-set boot on first run downloads a mongod binary
        pool: 'forks', // isolate replica sets per test file
    },
});
