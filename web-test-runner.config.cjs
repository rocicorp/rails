process.env.NODE_ENV = 'test';

/** @type {import('@web/test-runner').TestRunnerConfig} */
const config = {
  plugins: [require('@snowpack/web-test-runner-plugin')()],
  testFramework: {
    config: {
      ui: 'tdd',
    },
  },
  testRunnerHtml: testFramework =>
    `<!doctype html>
    <html>
    <body>
      <script>window.process = { env: { NODE_ENV: "development" } }</script>
      <script type="module" src="${testFramework}"></script>
    </body>
  </html>`,
};

module.exports = config;
