FROM node:20-alpine
WORKDIR /app
COPY services/order-service/package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY services/order-service/src ./src
COPY proto ./proto
ENV NODE_ENV=production
CMD ["node", "src/index.js"]
