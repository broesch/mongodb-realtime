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
});
