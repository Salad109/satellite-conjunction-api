package io.salad109.conjunctionapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.docs.Documenter;

@SpringBootTest
class ConjunctionApiApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    void verifiesModularStructure() {
        ApplicationModules modules = ApplicationModules.of(ConjunctionApiApplication.class);
        modules.verify();
    }

    @Test
    void writeDocumentationSnippets() {
        ApplicationModules modules = ApplicationModules.of(ConjunctionApiApplication.class);
        new Documenter(modules)
                .writeModulesAsPlantUml()
                .writeIndividualModulesAsPlantUml();
    }

}
