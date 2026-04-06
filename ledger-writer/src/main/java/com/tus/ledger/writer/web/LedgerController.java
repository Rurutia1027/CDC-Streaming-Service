package com.tus.ledger.writer.web;

import com.tus.ledger.writer.service.LedgerPostService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class LedgerController {

    private final LedgerPostService ledgerPostService;

    public LedgerController(LedgerPostService ledgerPostService) {
        this.ledgerPostService = ledgerPostService;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    @PostMapping("/ledger/post")
    public Map<String, Object> post(@RequestBody Map<String, Object> body) {
        String accountId = String.valueOf(body.get("accountId"));
        long amountCents = Long.parseLong(String.valueOf(body.get("amountCents")));
        return ledgerPostService.postTransaction(accountId, amountCents);
    }
}
