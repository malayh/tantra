FROM node:22-slim AS client
WORKDIR /repo/packages/tantra-acp-client
COPY packages/tantra-acp-client/package.json packages/tantra-acp-client/package-lock.json ./
RUN npm ci
COPY packages/tantra-acp-client/tsconfig.json ./
COPY packages/tantra-acp-client/src ./src
RUN npm run build

FROM node:22-slim AS deps
WORKDIR /repo
COPY packages/tantra-acp-client/package.json packages/tantra-acp-client/package.json
COPY --from=client /repo/packages/tantra-acp-client/dist packages/tantra-acp-client/dist
WORKDIR /repo/apps/sarathi/ui
COPY apps/sarathi/ui/package.json apps/sarathi/ui/yarn.lock ./
RUN yarn install --frozen-lockfile

FROM node:22-slim AS builder
WORKDIR /repo/apps/sarathi/ui
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=deps /repo/apps/sarathi/ui/node_modules ./node_modules
COPY --from=deps /repo/packages/tantra-acp-client /repo/packages/tantra-acp-client
COPY apps/sarathi/ui ./
RUN yarn build

FROM node:22-slim AS runner
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000 \
    HOSTNAME=0.0.0.0
COPY --from=builder /repo/apps/sarathi/ui/public ./apps/sarathi/ui/public
COPY --from=builder /repo/apps/sarathi/ui/.next/standalone ./
COPY --from=builder /repo/apps/sarathi/ui/.next/static ./apps/sarathi/ui/.next/static
EXPOSE 3000
CMD ["node", "apps/sarathi/ui/server.js"]
