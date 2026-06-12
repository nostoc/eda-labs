// audit.js
const amqp = require('amqplib');

// Define the existing exchange used by the producer 
const EXCHANGE = 'credit_events';

async function consume() {
    try {
        // Establish connection and channel 
        const connection = await amqp.connect('amqp://127.0.0.1');
        const channel = await connection.createChannel();

        // Assert the exchange as 'fanout' to receive broadcasted messages 
        await channel.assertExchange(EXCHANGE, 'fanout', { durable: false });

        // Create a temporary, exclusive queue for the audit service 
        const q = await channel.assertQueue('', { exclusive: true });

        // Bind the queue to the fanout exchange 
        await channel.bindQueue(q.queue, EXCHANGE, '');

        console.log('Audit Service Waiting for events...');

        // Consume the messages from the queue 
        channel.consume(
            q.queue,
            (msg) => {
                if (msg.content) {
                    const data = JSON.parse(msg.content.toString());

                    // Standard logging for the audit trail
                    console.log(`[AUDIT LOG] - ${new Date().toISOString()} | EVENT RECORDED for ${data.customerName}`);
                }
            },
            { noAck: true }
        );
    } catch (error) {
        console.error('Audit Service Error:', error);
    }
}

consume();