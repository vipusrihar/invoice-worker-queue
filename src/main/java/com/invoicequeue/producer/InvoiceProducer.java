package com.invoicequeue.producer;

import com.invoicequeue.model.InvoiceRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * InvoiceProducer — the message publisher.
 *
 * In the raw amqp-client version, this logic lived inside a main() method
 * that manually opened a Connection → Channel → called basicPublish().
 *
 * In Spring AMQP, RabbitTemplate handles all that for us. We just call
 * rabbitTemplate.convertAndSend() with the exchange, routing key, and payload.
 * Spring serializes the InvoiceRequest to JSON automatically (via Jackson2JsonMessageConverter).
 *
 * This class is a plain @Service — injected into the REST controller below.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceProducer {

    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbitmq.exchange.name}")
    private String exchangeName;

    @Value("${rabbitmq.routing.key}")
    private String routingKey;

    /**
     * Publishes a single invoice job to RabbitMQ.
     *
     * convertAndSend():
     *   1. Serializes InvoiceRequest → JSON bytes
     *   2. Sets message properties (content-type: application/json)
     *   3. Publishes to the exchange with the routing key
     *   4. Returns immediately — no waiting for a worker to finish
     *
     * This is the core of "asynchronous": the REST endpoint returns
     * a 202 Accepted in milliseconds, regardless of how long the
     * actual PDF generation takes.
     */
    public void publishInvoiceJob(InvoiceRequest request) {
        log.info("[PRODUCER] Publishing invoice job → ID: {}, Customer: {}, Type: {}, Complexity: '{}'",
                request.getInvoiceId(),
                request.getCustomerName(),
                request.getInvoiceType(),
                request.getComplexityDots());

        rabbitTemplate.convertAndSend(exchangeName, routingKey, request);

        log.info("[PRODUCER] ✅ Job accepted and queued → {}", request.getInvoiceId());
    }
}
