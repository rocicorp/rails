// @ts-check

/* eslint-env node, es2022 */

import {esbuildPlugin} from '@web/dev-server-esbuild';
import {importMapsPlugin} from '@web/dev-server-import-maps';
import {playwrightLauncher} from '@web/test-runner-playwright';

const chromium = playwrightLauncher({product: 'chromium'});
const webkit = playwrightLauncher({product: 'webkit'});
const firefox = playwrightLauncher({product: 'firefox'});

process.env.NODE_ENV = 'test';

/** @type {import('@web/test-runner').TestRunnerConfig} */
const config = {
  nodeResolve: true,
  plugins: [
    esbuildPlugin({
      ts: true,
      target: 'es2022',
      define: {
        'process.env.NODE_ENV': '"test"',
      },
    }),

    // For some unknown reason the test runner does not pick the "browser" field
    // for nanoid.
    importMapsPlugin({
      inject: {
        importMap: {
          imports: {nanoid: './node_modules/nanoid/index.browser.js'},
        },
      },
    }),
  ],
  testFramework: {
    config: {
      ui: 'tdd',
    },
  },
  files: ['src/**/*.test.ts'],
  browsers: [firefox, chromium, webkit],
};

export default config;
