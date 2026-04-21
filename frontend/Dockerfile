FROM node:20-slim
WORKDIR /app
ENV NODE_ENV=development \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_AUDIT=false \
    DANGEROUSLY_DISABLE_HOST_CHECK=true

COPY package.json package-lock.json* ./
RUN npm install --no-audit --no-fund --legacy-peer-deps

COPY . .
EXPOSE 3000
CMD ["npm", "start"]
