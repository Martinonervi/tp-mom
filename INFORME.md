## Decisiones de diseño

### prefetch_count: 1 en la cola, 0 en el exchange

El prefetch limita cuántos mensajes el broker entrega a un consumidor sin
recibir ack.

En la cola use 1. Varios consumidores compiten por la misma cola, así que
el broker reparte según la capacidad real de cada uno y no por cantidad: 
garantizando fairness, donde las tareas se reparten mejor. Por ejemplo
si a un worker le llega una tarea pesada, que no siga recibiendo mientras procesa, mejor delegarselo a otro.

En el exchange use 0 (sin límite). Cada suscriptor tiene su propia cola y
no compite con nadie, así que la distribución equitativa que compra el
prefetch bajo no aplica. Limitarlo sólo agregaría una espera de red por
mensaje sin ningún beneficio.

### Durabilidad: cola durable + mensajes persistentes

En la cola se declara `durable=True` y se publica con
`delivery_mode=Persistent`. Las dos mitades son necesarias: una cola durable
con mensajes transitorios vuelve vacía tras un reinicio del broker, y un
mensaje persistente no sobrevive si su cola no vuelve.

En el exchange, en cambio, se publica con `Transient`. Su cola es exclusiva y
no sobrevive a la desconexión, así que marcar los mensajes como persistentes
prometería una durabilidad que el contenedor no puede cumplir.

### Suscripción transitoria en el exchange

Cada instancia declara una cola exclusiva con nombre generado por el servidor,
que el broker elimina al cerrarse la conexión. Es una suscripción transitoria:
el suscriptor recibe sólo lo que se publica mientras está conectado y pierde
los eventos de la ventana en que estuvo caído.

La alternativa sería una cola durable por suscriptor, que no pierde eventos
pero necesita una identidad estable para reencontrar su cola al reconectarse
—y en el constructor de la clase no se pasa-. Además, las colas de los suscriptores que no vuelven seguirían bindeadas al
exchange acumulando mensajes indefinidamente.