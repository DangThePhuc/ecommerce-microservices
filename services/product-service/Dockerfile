FROM node:20-alpine
WORKDIR /app
COPY services/product-service/package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY services/product-service/src ./src
COPY proto ./proto
ENV NODE_ENV=production
CMD ["node", "src/index.js"]
