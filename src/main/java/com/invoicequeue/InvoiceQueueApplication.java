package com.invoicequeue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * InvoiceQueueApplication — Spring Boot entry point.
 *
 * When this starts:
 *   1. Spring connects to RabbitMQ (localhost:5672)
 *   2. RabbitMQConfig declares the queue, exchange, and binding
 *   3. InvoiceWorker starts listening on invoice_generation_queue
 *   4. InvoiceController exposes REST endpoints on port 8080
 *
 * Everything you need is in one application.
 * Run multiple instances on different ports to simulate competing workers.
 */
@SpringBootApplication
public class InvoiceQueueApplication {
    public static void main(String[] args) {
        SpringApplication.run(InvoiceQueueApplication.class, args);
    }
}
