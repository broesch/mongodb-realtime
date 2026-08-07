import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

// Regression coverage for a real bug: root package.json sets "type": "module", but
// dist/index.js is CJS output (tsup's outExtension puts require-format at .js to match
// the exports map). Without a dist/package.json override, Node treats dist/index.js as
// ESM by file-location rules and every `require()` of the built package crashes with
// "module is not defined in ES module scope". `node -e '...'` does NOT catch this — a
// -e script's module type is decided independently of any package.json, so this test
// spawns a genuine .cjs file instead.

const rootDir = fileURLToPath(new URL('..', import.meta.url));
const distIndexCjs = join(rootDir, 'dist', 'index.js');
const distIndexEsm = join(rootDir, 'dist', 'index.mjs');
const tsupBin = join(rootDir, 'node_modules', '.bin', 'tsup');

describe('built artifacts load in both module systems', () => {
    beforeAll(() => {
        if (!existsSync(distIndexCjs) || !existsSync(distIndexEsm)) {
            execFileSync(tsupBin, [], { cwd: rootDir, stdio: 'inherit' });
        }
    }, 60_000);

    it('requires cleanly from a genuine CommonJS file', () => {
        const probeDir = mkdtempSync(join(tmpdir(), 'mongodb-realtime-cjs-smoke-'));
        const probePath = join(probeDir, 'probe.cjs');
        writeFileSync(probePath, `require(${JSON.stringify(distIndexCjs)});\nconsole.log('CJS_REQUIRE_OK');\n`);

        try {
            const output = execFileSync(process.execPath, [probePath], { encoding: 'utf8' });
            expect(output).toContain('CJS_REQUIRE_OK');
        } finally {
            rmSync(probeDir, { recursive: true, force: true });
        }
    });

    it('dynamic-imports cleanly as ESM', async () => {
        await expect(import(distIndexEsm)).resolves.toBeDefined();
    });
});
