package io.salad109.conjunctionapi.conjunction;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/conjunctions")
public class ConjunctionController {

    private final ConjunctionService conjunctionService;

    public ConjunctionController(ConjunctionService conjunctionService) {
        this.conjunctionService = conjunctionService;
    }

    @GetMapping("/scan")
    public ResponseEntity<List<Void>> scanForConjunctions() {
        conjunctionService.findConjunctions();
        return ResponseEntity.ok().build();
    }
}