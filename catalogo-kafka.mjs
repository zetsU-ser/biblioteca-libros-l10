// L10 (grupos de 3): biblioteca-libros publica su catalogo en biblioteca.catalogo, un topic COMPACTADO con key
// libroId. Cada evento lleva el libro completo, asi que el ultimo de cada key es el estado del libro y Kafka
// puede borrar los anteriores sin perder nada.
//
//   libro.publicado    un libro que el topic todavia no tiene
//   libro.actualizado  un libro cuyo estado en el topic es distinto del de datos/catalogo.json
//
// Como sabe que ya publico: al arrancar lee su propio topic desde el offset 0 hasta el final (grupo
// biblioteca-libros, sin confirmar offsets). Asi, reiniciar el servicio no vuelve a publicar nada, y editar
// datos/catalogo.json publica solo lo que cambio (lo revisa cada 5 segundos). Un libro que se borra del JSON no
// se retira del topic: eso seria un tombstone (value null), que este laboratorio no usa.
import { randomUUID } from 'node:crypto';
import {
    Consumer,
    MessagesStreamModes,
    Producer,
    ProduceAcks,
    compatibilityPartitioner,
    stringDeserializers,
    stringSerializers,
} from '@platformatic/kafka';

const TOPIC = 'biblioteca.catalogo';
const CADA_MS = 5000;
const log = (texto) => console.log(`[libros] kafka: ${texto}`);

// Lo que viaja de cada libro (la sinopsis no: es larga y nadie la consume).
const datosDe = (l) => ({
    titulo: l.titulo,
    autor: l.autor,
    anio: l.anio ?? null,
    paginas: l.paginas ?? null,
    categoria: l.categoria,
    isbn13: l.isbn13 ?? null,
    ejemplares: l.ejemplares,
});

// Lee biblioteca.catalogo desde el offset 0 hasta el ultimo mensaje y devuelve el ultimo estado de cada libro.
async function leerEstadoPublicado(brokers) {
    const consumidor = new Consumer({
        groupId: 'biblioteca-libros',
        clientId: 'biblioteca-libros',
        bootstrapBrokers: brokers,
        deserializers: stringDeserializers,
    });
    const estado = new Map();
    try {
        const finales = (await consumidor.listOffsets({ topics: [TOPIC] })).get(TOPIC) ?? [];
        const pendientes = new Set(finales.flatMap((fin, particion) => (fin > 0n ? [particion] : [])));
        if (pendientes.size === 0) return estado;
        const flujo = await consumidor.consume({ topics: [TOPIC], autocommit: false, mode: MessagesStreamModes.EARLIEST });
        for await (const m of flujo) {
        if (m.value) estado.set(m.key, JSON.parse(m.value).datos);
        if (m.offset + 1n >= finales[m.partition]) pendientes.delete(m.partition);
        if (pendientes.size === 0) break;
        }
        await flujo.close().catch(() => undefined); // el break ya pudo cerrarlo
        return estado;
    } finally {
        await consumidor.close();
    }
}

export async function publicarCatalogo({ brokers, leerLibros }) {
    const publicado = await leerEstadoPublicado(brokers);
    log(`${publicado.size} libros ya estaban en ${TOPIC}`);

    const productor = new Producer({
        clientId: 'biblioteca-libros',
        bootstrapBrokers: brokers,
        serializers: stringSerializers,
        idempotent: true,
        acks: ProduceAcks.ALL,
        retries: 10,
        partitioner: compatibilityPartitioner,
        autocreateTopics: false,
    });

    // Compara el JSON con lo publicado y manda solo las diferencias, en un envio.
    async function sincronizar() {
        const mensajes = [];
        for (const libro of leerLibros()) {
        const libroId = String(libro.id);
        const datos = datosDe(libro);
        const anterior = publicado.get(libroId);
        if (anterior && JSON.stringify(anterior) === JSON.stringify(datos)) continue;
        const evento = {
            eventoId: randomUUID(),
            tipo: anterior ? 'libro.actualizado' : 'libro.publicado',
            version: 1,
            emitidoEn: new Date().toISOString(),
            libroId,
            datos,
        };
        mensajes.push({
            topic: TOPIC,
            key: libroId,
            value: JSON.stringify(evento),
            headers: { 'x-evento-id': evento.eventoId, 'x-correlation-id': randomUUID(), 'content-type': 'application/json' },
        });
        }
        if (mensajes.length === 0) return;
        await productor.send({ messages: mensajes });
        for (const { key, value } of mensajes) publicado.set(key, JSON.parse(value).datos);
        const tipos = mensajes.map((m) => JSON.parse(m.value).tipo);
        const publicados = tipos.filter((t) => t === 'libro.publicado').length;
        log(`${publicados} libro.publicado y ${tipos.length - publicados} libro.actualizado (libros ${mensajes.map((m) => m.key).join(', ')})`);
    }

    let enCurso = false;
    const revisar = async () => {
        if (enCurso) return;
        enCurso = true;
        try {
        await sincronizar();
        } catch (error) {
        log(`no se pudo publicar, se reintenta en ${CADA_MS / 1000} s: ${error.message}`);
        } finally {
        enCurso = false;
        }
    };
    await revisar();
    const reloj = setInterval(revisar, CADA_MS);
    return async () => {
        clearInterval(reloj);
        await productor.close();
    };
}