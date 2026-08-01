# Development image for the Sumi React + Vite frontend.
# In production the static build is served by a CDN/host; this image runs the
# Vite dev server for local development.
FROM node:22-bookworm-slim
WORKDIR /src

# Install pnpm via corepack.
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy workspace manifests and install all dependencies.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc* ./
RUN mkdir -p apps/api apps/agent apps/web
COPY apps/api/package.json ./apps/api/
COPY apps/agent/package.json ./apps/agent/
COPY apps/web/package.json ./apps/web/
COPY packages ./packages
RUN pnpm install --frozen-lockfile

# Copy the web source.
COPY apps/web/ ./apps/web/

WORKDIR /src/apps/web
EXPOSE 5173
CMD ["pnpm", "dev", "--host", "--port", "5173"]
