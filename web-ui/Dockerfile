FROM node:20-slim

WORKDIR /app

# Copy package.json first to leverage Docker cache for npm install
COPY package.json .
RUN npm install

# Copy all files
COPY . .

EXPOSE 3000

# Use a simple HTTP server to serve static files
CMD ["npx", "serve", "-s", ".", "-l", "3000"]