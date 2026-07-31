# syntax=docker/dockerfile:1

FROM node:22-alpine AS build

RUN corepack enable && corepack prepare pnpm@11.9.0 --activate
WORKDIR /src

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json ./
COPY apps/web/package.json ./apps/web/package.json
COPY packages/api-client/package.json ./packages/api-client/package.json
COPY packages/sdui/package.json ./packages/sdui/package.json
COPY packages/typescript-config/package.json ./packages/typescript-config/package.json
COPY packages/ui/package.json ./packages/ui/package.json
RUN pnpm install --frozen-lockfile --filter @sumi/web...

COPY apps/web/ ./apps/web/
COPY packages/ ./packages/
RUN pnpm --filter @sumi/web build

FROM nginx:1.27-alpine

COPY apps/web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /src/apps/web/dist /usr/share/nginx/html

EXPOSE 80
