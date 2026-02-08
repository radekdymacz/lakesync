# Stage 1: Install dependencies
FROM oven/bun:1.3 AS deps
WORKDIR /app
COPY package.json bun.lock* ./
COPY packages/core/package.json packages/core/package.json
COPY packages/adapter/package.json packages/adapter/package.json
COPY packages/gateway/package.json packages/gateway/package.json
COPY packages/catalogue/package.json packages/catalogue/package.json
COPY packages/parquet/package.json packages/parquet/package.json
COPY packages/gateway-server/package.json packages/gateway-server/package.json
RUN bun install --frozen-lockfile

# Stage 2: Copy source and run
FROM oven/bun:1.3 AS runtime
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

ENV PORT=3000
ENV GATEWAY_ID=default
EXPOSE 3000

# The server uses node:http which is compatible with both Node.js and Bun
CMD ["bun", "run", "packages/gateway-server/src/index.ts"]
