import * as amqp from 'amqplib';
import fetch from 'isomorphic-fetch'; // Importamos isomorphic-fetch en lugar de node-fetch

async function connect() {
    try {
        const connection = await amqp.connect('localhost');
        const channel = await connection.createChannel();

        // Declaramos la cola desde la que vamos a consumir
        const queueName = 'cola';
        await channel.assertQueue(queueName);

        console.log(`Esperando mensajes en la cola ${queueName}...`);

        // Consumimos los mensajes de la cola
        channel.consume(queueName, async (msg) => {
            if (msg !== null) {
                try {
                    // Aquí podrías procesar el mensaje según tus necesidades
                    console.log("Mensaje recibido:", msg.content.toString());

                    // Enviar el mensaje a una ruta específica
                    await enviarMensaje('http://localhost:3000/api/message', msg.content.toString());

                    // Confirmar que hemos procesado el mensaje
                    channel.ack(msg);
                } catch (error) {
                    console.error("Error al procesar el mensaje:", error);
                    // Rechazar el mensaje y devolverlo a la cola
                    channel.nack(msg);
                }
            }
        });
    } catch (error) {
        console.error('Error al conectar con RabbitMQ:', error);
    }
}

// Función para enviar un mensaje a una ruta específica
async function enviarMensaje(url: string, mensaje: string) {
    const headers: { [key: string]: string } = {
        'Content-Type': 'application/json'
    };

    const body = JSON.stringify({ mensaje });

    const options: { method: string, headers: any, body: string } = {
        method: 'POST',
        headers,
        body
    };

    try {
        const response = await fetch(url, options);
        if (response.ok) {
            console.log("Mensaje enviado correctamente.");
            connect();
        } else {
            throw new Error(`Error al enviar el mensaje: ${response.statusText}`);
        }
    } catch (error :any ) {
        throw new Error(`Error al enviar el mensaje: ${error.message}`);
    }
}

connect();
