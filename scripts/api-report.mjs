// Generates (or, with --verify, checks) the public-API reports for the four
// subpath entry points. The committed etc/*.api.md files are the reviewable
// record of the package's surface: a PR that changes them is a PR that
// changes the public API, and must say so (and bump semver accordingly).
import { execSync } from 'node:child_process';
import { Extractor, ExtractorConfig } from '@microsoft/api-extractor';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { rmSync } from 'node:fs';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const verify = process.argv.includes('--verify');

rmSync(path.join(root, '.api-tmp'), { recursive: true, force: true });
execSync('pnpm exec tsc -p tsconfig.api.json', { cwd: root, stdio: 'inherit' });

const entries = [
    { name: 'mongodb-realtime', dts: '.api-tmp/index.d.ts' },
    { name: 'mongodb-realtime-presence', dts: '.api-tmp/presence/index.d.ts' },
    { name: 'mongodb-realtime-socketio', dts: '.api-tmp/socketio/index.d.ts' },
    { name: 'mongodb-realtime-nestjs', dts: '.api-tmp/nestjs/index.d.ts' },
];

let failed = false;
for (const entry of entries) {
    const config = ExtractorConfig.prepare({
        configObject: {
            projectFolder: root,
            mainEntryPointFilePath: path.join(root, entry.dts),
            compiler: { tsconfigFilePath: path.join(root, 'tsconfig.api.json') },
            apiReport: {
                enabled: true,
                reportFileName: `${entry.name}.api.md`,
                reportFolder: path.join(root, 'etc'),
                // Without this, api-extractor's temp comparison copy (used by --verify)
                // lands at the repo root instead of somewhere gitignored.
                reportTempFolder: path.join(root, '.api-tmp', 'report-temp'),
            },
            docModel: { enabled: false },
            dtsRollup: { enabled: false },
            tsdocMetadata: { enabled: false },
            messages: {
                compilerMessageReporting: {
                    default: { logLevel: 'warning' },
                },
                extractorMessageReporting: {
                    default: { logLevel: 'warning' },
                    // Only actionable for consumers of the .d.ts rollup, which we don't
                    // produce (dtsRollup is disabled above).
                    'ae-forgotten-export': { logLevel: 'none' },
                    // We don't maintain TSDoc @public/@beta/@internal release tags across
                    // this internal package's surface; the api.md diff itself is the
                    // reviewable signal, not the tag hierarchy.
                    'ae-missing-release-tag': { logLevel: 'none' },
                    'ae-undocumented': { logLevel: 'none' },
                    // Flags a private class member (not part of any api.md); noise from
                    // analyzing internals that never reach the public surface.
                    'ae-missing-getter': { logLevel: 'none' },
                    // Each of the four subpaths is analyzed in isolation, so an @link
                    // pointing at a symbol exported from a *different* subpath's entry
                    // point can never resolve here. Not a doc error, just a byproduct of
                    // the multi-entry-point setup.
                    'ae-unresolved-link': { logLevel: 'none' },
                },
                tsdocMessageReporting: {
                    default: { logLevel: 'none' },
                },
            },
        },
        configObjectFullPath: undefined,
        packageJsonFullPath: path.join(root, 'package.json'),
    });
    const result = Extractor.invoke(config, { localBuild: !verify, showVerboseMessages: false });
    if (verify && (result.apiReportChanged || !result.succeeded)) failed = true;
    if (!verify && !result.succeeded) failed = true;
}
process.exit(failed ? 1 : 0);
