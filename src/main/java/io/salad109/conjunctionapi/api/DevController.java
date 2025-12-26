package io.salad109.conjunctionapi.api;

import io.salad109.conjunctionapi.conjunction.ConjunctionService;
import io.salad109.conjunctionapi.ingestion.IngestionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class DevController {

    private final IngestionService ingestionService;
    private final ConjunctionService conjunctionService;

    public DevController(IngestionService ingestionService, ConjunctionService conjunctionService) {
        this.ingestionService = ingestionService;
        this.conjunctionService = conjunctionService;
    }

    @PostMapping("/sync")
    public ResponseEntity<Void> sync() {
        ingestionService.sync();
        return ResponseEntity.ok().build();
    }


    @GetMapping("/scan")
    public ResponseEntity<List<Void>> scanForConjunctions() {
        conjunctionService.findConjunctions();
        return ResponseEntity.ok().build();
    }

    @GetMapping("/sync-and-scan")
    public ResponseEntity<List<Void>> syncAndScan() {
        ingestionService.sync();
        conjunctionService.findConjunctions();
        return ResponseEntity.ok().build();
    }
}
