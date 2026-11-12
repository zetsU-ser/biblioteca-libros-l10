# biblioteca-libros: Node plano, sin compilar. Sirve datos/catalogo.json en el 3001 y, si recibe
# KAFKA_BROKERS, publica el catalogo en biblioteca.catalogo (catalogo-kafka.mjs).
FROM node:24-slim
WORKDIR /app
ENV NODE_ENV=production
# Su unica dependencia de produccion es el cliente de Kafka.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY libros.mjs catalogo-kafka.mjs ./
COPY datos/ ./datos/
USER node
EXPOSE 3001
CMD ["node", "libros.mjs"]
