package com.invoicequeue.producer;

import com.invoicequeue.model.InvoiceRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * InvoiceController — the REST API layer (the entry point for clients).
 *
 * This is exactly what you already know from Spring Boot:
 * a @RestController with @PostMapping endpoints.
 *
 * The KEY difference from a traditional synchronous REST controller:
 *   → Instead of generating the PDF here and blocking the HTTP thread,
 *     we publish a message to RabbitMQ and return 202 Accepted immediately.
 *   → The actual PDF work happens asynchronously in InvoiceWorker.
 *
 * Client experience: submitting an invoice takes ~5ms (queue publish).
 * Without queuing: submitting an invoice takes 1–5 seconds (blocking PDF render).
 */
@Slf4j
@RestController
@RequestMapping("/api/invoices")
@RequiredArgsConstructor
public class InvoiceController {

    private final InvoiceProducer invoiceProducer;

    // ---------------------------------------------------------------
    //  POST /api/invoices/submit
    //  Submit a single invoice generation job
    // ---------------------------------------------------------------
    @PostMapping("/submit")
    public ResponseEntity<Map<String, String>> submitInvoice(@RequestBody InvoiceRequest request) {
        request.setSubmittedAt(LocalDateTime.now());

        invoiceProducer.publishInvoiceJob(request);

        // Return 202 Accepted — not 200 OK.
        // 202 means "I got your request and will process it, but it's not done yet."
        // This is the correct HTTP status for async operations.
        return ResponseEntity.accepted().body(Map.of(
                "status", "ACCEPTED",
                "invoiceId", request.getInvoiceId(),
                "message", "Invoice job queued successfully. Processing in background."
        ));
    }

    // ---------------------------------------------------------------
    //  POST /api/invoices/submit-batch
    //  Submit a pre-built batch of 5 invoice jobs for demo/testing
    // ---------------------------------------------------------------
    @PostMapping("/submit-batch")
    public ResponseEntity<Map<String, Object>> submitBatch() {
        List<InvoiceRequest> batch = List.of(
                InvoiceRequest.builder()
                        .invoiceId("INV-2024-1001")
                        .customerName("Lakehouse Retail Ltd")
                        .invoiceType(InvoiceRequest.InvoiceType.SIMPLE_RECEIPT)
                        .complexityDots(".")
                        .submittedAt(LocalDateTime.now())
                        .build(),

                InvoiceRequest.builder()
                        .invoiceId("INV-2024-1002")
                        .customerName("BluePeak Exports")
                        .invoiceType(InvoiceRequest.InvoiceType.ITEMIZED_WITH_TAX)
                        .complexityDots("...")
                        .submittedAt(LocalDateTime.now())
                        .build(),

                InvoiceRequest.builder()
                        .invoiceId("INV-2024-1003")
                        .customerName("Fernridge Catering Co.")
                        .invoiceType(InvoiceRequest.InvoiceType.SIMPLE_RECEIPT)
                        .complexityDots(".")
                        .submittedAt(LocalDateTime.now())
                        .build(),

                InvoiceRequest.builder()
                        .invoiceId("INV-2024-1004")
                        .customerName("Montara Global Finance")
                        .invoiceType(InvoiceRequest.InvoiceType.ANNUAL_REPORT_INVOICE)
                        .complexityDots(".....")
                        .submittedAt(LocalDateTime.now())
                        .build(),

                InvoiceRequest.builder()
                        .invoiceId("INV-2024-1005")
                        .customerName("Coastline Marketing Agency")
                        .invoiceType(InvoiceRequest.InvoiceType.STANDARD_INVOICE)
                        .complexityDots("..")
                        .submittedAt(LocalDateTime.now())
                        .build()
        );

        batch.forEach(invoiceProducer::publishInvoiceJob);

        return ResponseEntity.accepted().body(Map.of(
                "status", "ACCEPTED",
                "jobsQueued", batch.size(),
                "message", "Batch of " + batch.size() + " invoice jobs queued. Workers are processing."
        ));
    }
}
