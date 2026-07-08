package com.invoicequeue.consumer;

import com.invoicequeue.model.InvoiceRequest;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * InvoiceWorker — the message consumer (the "worker" in the Work Queue pattern).
 *
 * In the raw amqp-client version, we wrote a DeliverCallback manually,
 * called channel.basicConsume(), and managed the connection lifecycle ourselves.
 *
 * In Spring AMQP, we simply annotate a method with @RabbitListener.
 * Spring Boot handles:
 *   → Connecting to RabbitMQ on startup
 *   → Spawning listener threads (controlled by spring.rabbitmq.listener.simple.concurrency)
 *   → Deserializing the JSON message body back into an InvoiceRequest object
 *   → Passing us the raw Channel for manual acknowledgment
 *
 * COMPETING CONSUMERS:
 *   If you run multiple instances of this application (or set concurrency > 1
 *   in application.properties), multiple workers compete for messages from
 *   the same queue. RabbitMQ guarantees each message is delivered to exactly
 *   one worker — this is the Competing Consumers pattern.
 *
 * FAIR DISPATCH:
 *   spring.rabbitmq.listener.simple.prefetch=1 in application.properties maps
 *   to basicQos(1) in the raw client. A worker only receives the next message
 *   after it has acknowledged the current one.
 */
@Slf4j
@Component
public class InvoiceWorker {

    /**
     * @RabbitListener wires this method to the invoice_generation_queue.
     *
     * Parameters explained:
     *   InvoiceRequest request     — Spring auto-deserializes the JSON message body
     *   Channel channel            — raw AMQP channel, needed for manual ack
     *   @Header deliveryTag        — unique ID of this message delivery (used in ack/nack)
     */
    @RabbitListener(queues = "${rabbitmq.queue.name}")
    public void processInvoice(
            InvoiceRequest request,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {

        String workerThread = Thread.currentThread().getName();

        log.info("[WORKER: {}] 📥 Received job → ID: {}, Customer: {}, Type: {}, Complexity: '{}'",
                workerThread,
                request.getInvoiceId(),
                request.getCustomerName(),
                request.getInvoiceType(),
                request.getComplexityDots());

        try {
            // Simulate the actual PDF generation work
            generateInvoicePdf(request, workerThread);

            // -------------------------------------------------------
            //  MANUAL ACKNOWLEDGMENT (the critical part)
            //
            //  basicAck(deliveryTag, multiple=false):
            //    → Tells RabbitMQ: "I finished this job successfully."
            //    → RabbitMQ removes the message from the queue permanently.
            //    → Only called AFTER the work is complete — not before.
            //
            //  If this line is never reached (exception, crash, Ctrl+C):
            //    → RabbitMQ detects the connection drop.
            //    → The message is automatically re-queued.
            //    → Another available worker picks it up.
            // -------------------------------------------------------
            channel.basicAck(deliveryTag, false);

            log.info("[WORKER: {}] ✅ Invoice complete & acknowledged → {}",
                    workerThread, request.getInvoiceId());

        } catch (Exception e) {
            log.error("[WORKER: {}] ❌ Error processing {} → {}",
                    workerThread, request.getInvoiceId(), e.getMessage());

            // -------------------------------------------------------
            //  NEGATIVE ACKNOWLEDGMENT on failure
            //
            //  basicNack(deliveryTag, multiple=false, requeue=true):
            //    → Tells RabbitMQ: "I couldn't handle this job."
            //    → requeue=true: puts the message back in the queue
            //      so another worker can retry it.
            //    → requeue=false: discards the message (or sends to
            //      a Dead Letter Queue if one is configured).
            // -------------------------------------------------------
            channel.basicNack(deliveryTag, false, true);

            log.warn("[WORKER: {}] ⚠️ Message re-queued → {}", workerThread, request.getInvoiceId());
        }
    }

    /**
     * Simulates PDF rendering time.
     * Each '.' in complexityDots represents 1 second of rendering work.
     *
     * Real-world equivalent: calling a PDF library, fetching customer data,
     * applying templates, watermarking, then uploading the result to S3.
     */
    private void generateInvoicePdf(InvoiceRequest request, String workerThread)
            throws InterruptedException {

        String dots = request.getComplexityDots();
        int totalSeconds = (int) dots.chars().filter(c -> c == '.').count();

        log.info("[WORKER: {}] 🔄 Rendering {} — estimated {} second(s)...",
                workerThread, request.getInvoiceId(), totalSeconds);

        for (int i = 1; i <= totalSeconds; i++) {
            Thread.sleep(1000);
            log.info("[WORKER: {}] ⏳ {}/{} seconds — rendering {} ...",
                    workerThread, i, totalSeconds, request.getInvoiceId());
        }

        log.info("[WORKER: {}] 📄 PDF rendered for customer: {}",
                workerThread, request.getCustomerName());
    }
}
