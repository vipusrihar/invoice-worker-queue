package com.invoicequeue.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Represents an invoice generation job that travels through the queue.
 *
 * This object is serialized to JSON when published to RabbitMQ,
 * and deserialized back to a Java object when a worker picks it up.
 *
 * Think of it as the "request body" of an async operation —
 * equivalent to a @RequestBody in a Spring REST controller,
 * but it lives in a queue instead of an HTTP request.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InvoiceRequest implements Serializable {

    /** Unique invoice identifier (e.g. INV-2024-1001) */
    private String invoiceId;

    /** Customer this invoice belongs to */
    private String customerName;

    /** Invoice type determines how complex (and slow) the PDF render is */
    private InvoiceType invoiceType;

    /** Dots represent seconds of rendering work — one dot = 1 second */
    private String complexityDots;

    /** Timestamp when the REST API accepted this request */
    private LocalDateTime submittedAt;

    // ---------------------------------------------------------------
    //  Invoice types — each maps to a different rendering complexity
    // ---------------------------------------------------------------
    public enum InvoiceType {
        SIMPLE_RECEIPT,          // e.g. "."     → 1 second
        STANDARD_INVOICE,        // e.g. ".."    → 2 seconds
        ITEMIZED_WITH_TAX,       // e.g. "..."   → 3 seconds
        ANNUAL_REPORT_INVOICE    // e.g. "....." → 5 seconds
    }
}
