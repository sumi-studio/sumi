# Development image for the Sumi React + Vite frontend.
# Production serves a static build elsewhere; this image is intentionally the
# local development server used by the full Compose stack.
FROM node:22-bookworm-slim
WORKDIR /src

RUN corepack enable && corepack prepare pnpm@latest --activate

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc* ./
RUN mkdir -p apps/api apps/agent apps/web
COPY apps/api/package.json ./apps/api/
COPY apps/agent/package.json ./apps/agent/
COPY apps/web/package.json ./apps/web/
COPY packages ./packages
RUN pnpm install --frozen-lockfile

COPY apps/web/ ./apps/web/

WORKDIR /src/apps/web
EXPOSE 5173
CMD ["pnpm", "dev", "--host", "0.0.0.0", "--port", "5173"]
