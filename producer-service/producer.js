const express = require('express');
const amqp = require('amqplib');

const app = express();
app.use(express.json());

const EXCHANGE = 'credit_events';

async function publishEvent(data) {
    const connection = await amqp.connect('amqp://127.0.0.1');
    const channel = await connection.createChannel();

    await channel.assertExchange(EXCHANGE, 'fanout', {
        durable: false
    });

    channel.publish(
        EXCHANGE,
        '',
        Buffer.from(JSON.stringify(data))
    );

    console.log('Credit Alert Published');

    setTimeout(() => {
        connection.close();
    }, 500);
}

app.post('/credit-alert', async (req, res) => {
    await publishEvent(req.body);
    res.send('Event Published');
});

app.listen(3000, () => {
    console.log('Producer running on port 3000');
});