package com.invoicequeue.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Central RabbitMQ configuration class.
 *
 * In the raw amqp-client world, you declared queues and exchanges manually
 * inside each Producer/Consumer class with channel.queueDeclare().
 *
 * In Spring AMQP, you declare them here as @Beans — Spring Boot auto-creates
 * them in RabbitMQ on startup if they don't already exist. No boilerplate
 * connection code needed in your business classes.
 */
@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.queue.name}")
    private String queueName;

    @Value("${rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${rabbitmq.routing.key}")
    private String routingKey;

    // ---------------------------------------------------------------
    //  Queue — durable:true means it survives a RabbitMQ restart
    // ---------------------------------------------------------------
    @Bean
    public Queue invoiceQueue() {
        return QueueBuilder
                .durable(queueName)   // persisted to disk, survives broker restart
                .build();
    }

    // ---------------------------------------------------------------
    //  Exchange — a Direct Exchange routes messages by exact routing key
    //  (think of it as a "dispatcher" that decides which queue gets the message)
    // ---------------------------------------------------------------
    @Bean
    public DirectExchange invoiceExchange() {
        return new DirectExchange(exchangeName);
    }

    // ---------------------------------------------------------------
    //  Binding — connects the queue to the exchange via a routing key.
    //  Only messages published with routingKey="invoice.generate"
    //  will land in invoice_generation_queue.
    // ---------------------------------------------------------------
    @Bean
    public Binding invoiceBinding(Queue invoiceQueue, DirectExchange invoiceExchange) {
        return BindingBuilder
                .bind(invoiceQueue)
                .to(invoiceExchange)
                .with(routingKey);
    }

    // ---------------------------------------------------------------
    //  Message Converter — serialize InvoiceRequest to JSON automatically.
    //  Without this, Spring AMQP would use Java serialization (binary, fragile).
    //  With this, your messages look like: {"invoiceId":"INV-1001", ...}
    // ---------------------------------------------------------------
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // ---------------------------------------------------------------
    //  RabbitTemplate — the Spring way to SEND messages.
    //  Equivalent to channel.basicPublish() in the raw client.
    //  Inject this wherever you need to publish.
    // ---------------------------------------------------------------
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jsonMessageConverter());
        return rabbitTemplate;
    }
}
