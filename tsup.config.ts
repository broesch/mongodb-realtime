import { writeFile } from 'node:fs/promises';
import { defineConfig } from 'tsup';

export default defineConfig({
    entry: {
        index: 'src/index.ts',
        'presence/index': 'src/presence/index.ts',
        'socketio/index': 'src/socketio/index.ts',
        'nestjs/index': 'src/nestjs/index.ts',
    },
    format: ['cjs', 'esm'],
    dts: true,
    sourcemap: true,
    clean: true,
    target: 'node20',
    external: ['mongodb', 'socket.io', '@nestjs/common'],
    // package.json sets "type": "module", which flips tsup's default extensions
    // (.js for esm / .cjs for cjs). Force the layout the "exports" map expects instead:
    // .js for require (cjs), .mjs for import (esm).
    outExtension({ format }) {
        return { js: format === 'cjs' ? '.js' : '.mjs' };
    },
    // Root package.json says "type": "module", so Node treats every .js file under
    // dist/ as ESM by default — including the CJS output outExtension puts at .js.
    // A dist/-scoped package.json override (nearest one wins) tells Node those .js
    // files are CommonJS; .mjs files are unambiguous regardless. Written in onSuccess
    // (not a static file) because `clean: true` wipes dist/ before every build.
    async onSuccess() {
        await writeFile('dist/package.json', JSON.stringify({ type: 'commonjs' }, null, 4) + '\n');
    },
});
