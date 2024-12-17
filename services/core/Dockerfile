# Use official Node.js version 18 image as base
FROM node:18-alpine AS builder

RUN apk --no-cache add sqlite
RUN apk add --no-cache libc6-compat

# Set working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json to the container
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy the rest of the application code to the container
COPY . .

# Build TypeScript files
RUN npm run build

# Stage 2: Use a smaller base image
FROM node:18-alpine

# Set working directory in the container
WORKDIR /app

# Copy built files and necessary dependencies from the previous stage
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./package.json

# Expose ports 8080 and 3000
EXPOSE 8080
EXPOSE 3000

# Command to run the server
CMD ["node", "dist/core.js"]
