require('dotenv').config();

module.exports = {

    // RabbitMQ connection URL
    url: `amqp://${process.env.RABBITMQ_USERNAME}:${process.env.RABBITMQ_PASSWORD}@${process.env.RABBITMQ_HOST}:${process.env.RABBITMQ_PORT}`,

    // Queue configuration
    queue: {
        name: process.env.RABBITMQ_QUEUE_NAME || 'notifications',
        options: {
            durable: true
        }
    },

    // Message Options
    messageOptions: {
        persistent: true
    },

    // Consumer options
    consumerOptions: {
        noAck: false
    },

    // Publisher Port
    publisherPort: process.env.PUBLISHER_PORT || 3000,
}