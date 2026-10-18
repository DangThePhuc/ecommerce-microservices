FROM node:20-alpine
WORKDIR /app
COPY api-gateway/package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY api-gateway/src ./src
COPY proto ./proto
ENV NODE_ENV=production
EXPOSE 8080
CMD ["node", "src/index.js"]
