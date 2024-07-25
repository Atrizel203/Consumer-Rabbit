const amqplib = require('amqplib');

let isProcessing = false; // Flag to check if the system is processing a message
const processingTimeout = 5000; // Time in milliseconds to wait before processing the next message

async function main() {
    const queue = 'cola';
    try {
        const conn = await amqplib.connect('amqp://52.1.210.218');
        console.log("Conexion exitosa");

        const ch1 = await conn.createChannel();
        await ch1.assertQueue(queue);

        ch1.consume(queue, async (msg) => {
            if (msg !== null && !isProcessing) {
                isProcessing = true; // Set the flag to true to prevent further processing

                try {
                    const mensaje = JSON.parse(msg.content.toString());
                    console.log("Mensaje recibido:", mensaje);

                    if (!mensaje.fecha_envio) {
                        mensaje.fecha_envio = new Date().toISOString();
                    }

                    

                    ch1.ack(msg); // Acknowledge the message

                    setTimeout(() => {
                        isProcessing = false; // Reset the flag after the timeout
                    }, processingTimeout);
                } catch (error) {
                    console.error("Error al procesar el mensaje:", error);
                    ch1.nack(msg); // Nack the message if there was an error
                }
            }
        });

    } catch (error) {
        console.error("Error al conectar con RabbitMQ:", error);
    }
}

async function enviarMensaje(url, mensaje) {
    const headers = {
        'Content-Type': 'application/json'
    };

    const body = JSON.stringify(mensaje);
    console.log("Enviando mensaje a la API:", body);

    const options = {
        method: 'POST',
        headers: headers,
        body: body
    };

    try {
        const fetch = (await import('node-fetch')).default;
        const response = await fetch(url, options);
        if (response.ok) {
            console.log("Mensaje enviado correctamente.");
        } else {
            const errorResponse = await response.text();
            console.error(`Error al enviar el mensaje: ${response.statusText}, Response: ${errorResponse}`);
            throw new Error(`Error al enviar el mensaje: ${response.statusText}`);
        }
    } catch (error) {
        console.error(`Error en fetch: ${error}`);
        throw new Error(`Error al enviar el mensaje: ${error.message}`);
    }
}

main();
