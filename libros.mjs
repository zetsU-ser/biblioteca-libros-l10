import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { publicarCatalogo } from './catalogo-kafka.mjs';

// Se lee en cada peticion a proposito: asi editar el JSON no obliga a reiniciar el servicio.
const leer = () =>
  JSON.parse(readFileSync(new URL('./datos/catalogo.json', import.meta.url), 'utf8')).libros;

// Latencia simulada. En produccion este servicio estaria en otra maquina y la red costaria algo.
const LATENCIA_SIMULADA_MS = 300;

createServer(async (peticion, respuesta) => {
  await new Promise((listo) => setTimeout(listo, LATENCIA_SIMULADA_MS));
  const libros = leer();
  console.log(`[libros] ${peticion.method} ${peticion.url} -> ${libros.length}`);
  respuesta.writeHead(200, { 'Content-Type': 'application/json' });
  respuesta.end(JSON.stringify(libros));
}).listen(3001, () => console.log('microservicio de libros escuchando en http://localhost:3001'));

// L10: el catalogo tambien se publica en Kafka (biblioteca.catalogo). Sin KAFKA_BROKERS, el servicio sirve igual
// y no publica: asi sigue corriendo con `npm start` como en L1. Si Kafka no responde al arrancar, reintenta.
function arrancarKafka() {
  publicarCatalogo({ brokers: process.env.KAFKA_BROKERS.split(','), leerLibros: leer }).then(
    (cerrar) => {
      const salir = () => cerrar().finally(() => process.exit(0));
      process.once('SIGTERM', salir);
      process.once('SIGINT', salir);
    },
    (error) => {
      console.error(`[libros] kafka: no se pudo leer biblioteca.catalogo, se reintenta en 10 s: ${error.message}`);
      setTimeout(arrancarKafka, 10_000);
    },
  );
}
if (process.env.KAFKA_BROKERS) arrancarKafka();