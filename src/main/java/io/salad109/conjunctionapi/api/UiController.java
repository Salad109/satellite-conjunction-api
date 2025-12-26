package io.salad109.conjunctionapi.api;

import io.salad109.conjunctionapi.conjunction.ConjunctionService;
import io.salad109.conjunctionapi.ingestion.IngestionLogService;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/ui")
public class UiController {

    private final ConjunctionService conjunctionService;
    private final IngestionLogService ingestionLogService;

    public UiController(ConjunctionService conjunctionService, IngestionLogService ingestionLogService) {
        this.conjunctionService = conjunctionService;
        this.ingestionLogService = ingestionLogService;
    }

    @GetMapping
    public String getIndex() {
        return "index";
    }

    @GetMapping("/conjunctions")
    public String getConjunctions(@PageableDefault(sort = "tca", direction = Sort.Direction.DESC) Pageable pageable,
                                  Model model) {
        model.addAttribute("page", conjunctionService.getConjunctions(pageable));

        Sort.Order order = pageable.getSort().stream().findFirst().orElse(null);
        model.addAttribute("sortField", order != null ? order.getProperty() : "tca");
        model.addAttribute("sortDir", order != null ? order.getDirection().name().toLowerCase() : "desc");

        return "fragments/conjunction-table";
    }
}
