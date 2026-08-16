package org.stevearmstrong.ledgerguard.demo.scenario;

import java.math.BigDecimal;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record TransactionRequest(
        @Size(max = 40)
        @Pattern(regexp = "^[A-Za-z0-9._-]*$", message = "transactionId may contain letters, numbers, dots, underscores, and dashes")
        String transactionId,

        @DecimalMin(value = "0.01", message = "paymentAmount must be at least 0.01")
        @Digits(integer = 12, fraction = 2)
        BigDecimal paymentAmount,

        @Pattern(regexp = "^[A-Za-z]{3}$", message = "paymentCurrency must be a three-letter ISO code")
        String paymentCurrency,

        @DecimalMin(value = "0.01", message = "ledgerAmount must be at least 0.01")
        @Digits(integer = 12, fraction = 2)
        BigDecimal ledgerAmount,

        @Pattern(regexp = "^[A-Za-z]{3}$", message = "ledgerCurrency must be a three-letter ISO code")
        String ledgerCurrency,

        @NotNull
        EventOrder eventOrder,

        @Min(0)
        @Max(3000)
        long eventDelayMs,

        boolean duplicatePayment
) {
}
