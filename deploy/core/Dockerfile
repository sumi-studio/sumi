# Local multi-secretary host for the same Core deployed to Cloudflare.
FROM node:22.23.4-bookworm-slim
WORKDIR /opt/sumi
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile --filter @sumi/agent-core...
USER node
CMD ["node", "apps/core/src/host/dev-pool.ts"]
