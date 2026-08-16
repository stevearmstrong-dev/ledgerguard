package org.stevearmstrong.ledgerguard.demo.web;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;
import org.stevearmstrong.ledgerguard.demo.result.ReconciliationResultStore;
import org.stevearmstrong.ledgerguard.demo.scenario.ScenarioPublisher;
import org.stevearmstrong.ledgerguard.demo.scenario.ScenarioType;
import org.stevearmstrong.ledgerguard.demo.scenario.SubmissionResponse;
import org.stevearmstrong.ledgerguard.demo.scenario.TransactionRequest;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api")
public class LedgerGuardController {

    private final ScenarioPublisher scenarioPublisher;
    private final ReconciliationResultStore resultStore;

    public LedgerGuardController(
            ScenarioPublisher scenarioPublisher,
            ReconciliationResultStore resultStore
    ) {
        this.scenarioPublisher = scenarioPublisher;
        this.resultStore = resultStore;
    }

    @GetMapping
    Map<String, Object> index() {
        return Map.of(
                "service", "LedgerGuard Demo API",
                "description", "Real-time financial transaction reconciliation with Java and Kafka",
                "scenarioEndpoint", "POST /api/scenarios/{scenario}",
                "transactionEndpoint", "POST /api/transactions",
                "resultsEndpoint", "GET /api/reconciliations"
        );
    }

    @GetMapping("/scenarios")
    List<String> scenarios() {
        return Arrays.stream(ScenarioType.values())
                .map(Enum::name)
                .toList();
    }

    @PostMapping("/scenarios/{scenario}")
    SubmissionResponse runScenario(@PathVariable String scenario) {
        return scenarioPublisher.publish(parseScenario(scenario));
    }

    @PostMapping("/transactions")
    SubmissionResponse publishTransaction(@Valid @RequestBody TransactionRequest request) {
        return scenarioPublisher.publish(request);
    }

    @GetMapping("/reconciliations")
    List<ReconciliationResult> reconciliations(
            @RequestParam(required = false) String transactionId
    ) {
        return resultStore.findAll(transactionId);
    }

    @DeleteMapping("/reconciliations")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void clearReconciliations() {
        resultStore.clear();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail invalidTransaction(IllegalArgumentException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, exception.getMessage());
    }

    private ScenarioType parseScenario(String scenario) {
        try {
            return ScenarioType.valueOf(
                    scenario.trim().replace('-', '_').toUpperCase(Locale.ROOT)
            );
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Unknown scenario. Use GET /api/scenarios to list valid values."
            );
        }
    }
}
