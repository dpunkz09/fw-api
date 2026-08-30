module.exports = {
  apps: [
    {
      name: "fw-api",
      script: "server.js",
      interpreter: "node",
      watch: false,
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
