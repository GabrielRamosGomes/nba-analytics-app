FROM node:22-alpine AS build
WORKDIR /app

# Enable corepack
RUN corepack enable

# Copy package files first for better caching
COPY --link package.json pnpm-lock.yaml ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy the rest of the source
COPY --link . .
ENV NODE_ENV=production

# Build the production assets
RUN pnpm run build
RUN pnpm prune

# 2) Production stage
FROM nginx:alpine

# Remove default nginx content and copy built files 
RUN rm -rf /usr/share/nginx/html/*
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
