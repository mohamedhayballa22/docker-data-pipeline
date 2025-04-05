FROM node:14-alpine as build

WORKDIR /app

# Install a simple HTTP server for serving static content
RUN npm install -g http-server

COPY . .

# No build step needed for this simple example
# Just use the static HTML/JS

FROM nginx:alpine
COPY --from=build /app /usr/share/nginx/html
EXPOSE 80