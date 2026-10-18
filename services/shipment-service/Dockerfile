FROM node:20-alpine
WORKDIR /app
COPY services/shipment-service/package.json ./
RUN npm install --omit=dev
COPY services/shipment-service/src ./src
COPY proto ./proto
CMD ["node", "src/index.js"]
