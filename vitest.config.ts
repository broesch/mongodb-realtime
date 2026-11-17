import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        globals: true,
        globalSetup: ['./tests/global-setup.ts'], // one mongod download before the forks start
        testTimeout: 60_000,
        hookTimeout: 120_000,
        pool: 'forks', // isolate replica sets per test file
        exclude: ['**/node_modules/**', '**/dist/**', '**/.claude/**', '**/.api-tmp/**'],
    },
});
