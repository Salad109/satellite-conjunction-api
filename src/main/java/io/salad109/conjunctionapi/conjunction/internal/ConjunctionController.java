package io.salad109.conjunctionapi.conjunction.internal;

import io.salad109.conjunctionapi.conjunction.ConjunctionService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/conjunctions")
public class ConjunctionController {

    private final ConjunctionService conjunctionService;

    public ConjunctionController(ConjunctionService conjunctionService) {
        this.conjunctionService = conjunctionService;
    }

    @GetMapping
    public ResponseEntity<Page<ConjunctionInfo>> getConjunctions(
            @PageableDefault(sort = "tca", direction = Sort.Direction.DESC) Pageable pageable, boolean includeFormations) {
        return ResponseEntity.ok(conjunctionService.getConjunctions(pageable, includeFormations));
    }
}
