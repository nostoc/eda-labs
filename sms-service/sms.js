const amqp = require('amqplib');

const EXCHANGE = 'credit_events';

async function consume() {

    const connection =
        await amqp.connect('amqp://127.0.0.1');

    const channel =
        await connection.createChannel();

    await channel.assertExchange(
        EXCHANGE,
        'fanout',
        { durable: false }
    );

    const q =
        await channel.assertQueue(
            '',
            { exclusive: true }
        );

    await channel.bindQueue(
        q.queue,
        EXCHANGE,
        ''
    );

    console.log(
        'SMS Service Waiting...'
    );

    channel.consume(
        q.queue,
        (msg) => {

            const data =
                JSON.parse(
                    msg.content.toString()
                );

            console.log(
                `SMS SENT to ${data.customerName}`
            );

        },
        { noAck: true }
    );
}

consume();