FROM node:20-alpine
WORKDIR /app
COPY services/auth-service/package.json ./
RUN npm install --omit=dev
COPY services/auth-service/src ./src
COPY proto ./proto
CMD ["node", "src/index.js"]
