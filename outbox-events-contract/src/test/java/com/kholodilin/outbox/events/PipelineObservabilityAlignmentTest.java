package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares servlet, VT, and reactive sources against the shared log/trace vocabulary.
 */
class PipelineObservabilityAlignmentTest {

    private static final Pattern VOCABULARY_TOKEN = Pattern.compile("ObservabilityVocabulary\\.([A-Z0-9_]+)");

    @Test
    void orderPipelinesShareEventActionsAndSpanNames() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Set<String> expected = new LinkedHashSet<>(ObservabilityVocabulary.ORDER_SOURCE_TOKENS);
        for (String module : List.of("order-service", "order-service-vt", "order-service-reactive")) {
            String sources = readMainJava(repoRoot.resolve(module));
            assertThat(extractTokens(sources))
                    .as("module %s must emit the shared order observability tokens", module)
                    .containsAll(expected);
            assertThat(sources)
                    .as("module %s must copy Micrometer ids into trace.id/span.id", module)
                    .contains("enrichTracingAliases");
        }
    }

    @Test
    void stubPipelinesShareEventActionsAndSpanNames() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Set<String> expected = new LinkedHashSet<>(ObservabilityVocabulary.STUB_SOURCE_TOKENS);
        for (String module : List.of("notification-stub", "notification-stub-reactive")) {
            assertThat(extractTokens(readMainJava(repoRoot.resolve(module))))
                    .as("module %s must emit the shared stub observability tokens", module)
                    .containsAll(expected);
        }
    }

    @Test
    void reactiveOrderPropagatesMdcAndServletStubDisablesListenerObservation() throws IOException {
        Path repoRoot = resolveRepoRoot();
        String reactiveOrderYaml = Files.readString(
                repoRoot.resolve("order-service-reactive/src/main/resources/application.yml"),
                StandardCharsets.UTF_8);
        String servletStubYaml = Files.readString(
                repoRoot.resolve("notification-stub/src/main/resources/application.yml"),
                StandardCharsets.UTF_8);

        assertThat(reactiveOrderYaml).contains("context-propagation: auto");
        assertThat(servletStubYaml).contains("observation-enabled: false");
    }

    @Test
    void tracingDashboardsIncludeSharedBusinessSpanNames() throws IOException {
        Path repoRoot = resolveRepoRoot();
        String servletDashboard = Files.readString(
                repoRoot.resolve("monitoring/grafana/provisioning/dashboards/tracing-dashboard.json"),
                StandardCharsets.UTF_8);
        String reactiveDashboard = Files.readString(
                repoRoot.resolve("monitoring/grafana/provisioning/dashboards/tracing-dashboard-reactive.json"),
                StandardCharsets.UTF_8);

        for (String spanName : ObservabilityVocabulary.ORDER_SPAN_NAMES) {
            assertThat(servletDashboard).contains(spanName);
            assertThat(reactiveDashboard).contains(spanName);
        }
        for (String spanName : ObservabilityVocabulary.STUB_SPAN_NAMES) {
            assertThat(servletDashboard).contains(spanName);
            assertThat(reactiveDashboard).contains(spanName);
        }
        assertThat(servletDashboard).contains("order-service-vt");
        assertThat(servletDashboard).contains("outbox.*|batch.*");
        assertThat(reactiveDashboard).contains("outbox.*|batch.*");
    }

    @Test
    void vocabularyListsMatchSourceTokens() {
        assertThat(ObservabilityVocabulary.ORDER_EVENT_ACTIONS).hasSize(10);
        assertThat(ObservabilityVocabulary.ORDER_SPAN_NAMES).containsExactly(
                ObservabilityVocabulary.SPAN_OUTBOX_SAVE,
                ObservabilityVocabulary.SPAN_BATCH_FETCH,
                ObservabilityVocabulary.SPAN_BATCH_PUBLISH,
                ObservabilityVocabulary.SPAN_BATCH_COMPLETE,
                ObservabilityVocabulary.SPAN_OUTBOX_PUBLISH
        );
        assertThat(ObservabilityVocabulary.STUB_EVENT_ACTIONS).hasSize(6);
        assertThat(ObservabilityVocabulary.STUB_SPAN_NAMES).containsExactly(
                ObservabilityVocabulary.SPAN_NOTIFICATION_BATCH_RECEIVE,
                ObservabilityVocabulary.SPAN_NOTIFICATION_CONSUME
        );
    }

    private static Set<String> extractTokens(String sources) {
        Matcher matcher = VOCABULARY_TOKEN.matcher(sources);
        Set<String> tokens = new LinkedHashSet<>();
        while (matcher.find()) {
            tokens.add(matcher.group(1));
        }
        return tokens;
    }

    private static Path resolveRepoRoot() {
        Path cwd = Path.of("").toAbsolutePath();
        if (Files.isDirectory(cwd.resolve("order-service"))) {
            return cwd;
        }
        Path parent = cwd.getParent();
        if (parent != null && Files.isDirectory(parent.resolve("order-service"))) {
            return parent;
        }
        throw new IllegalStateException("Cannot resolve repository root from " + cwd);
    }

    private static String readMainJava(Path moduleRoot) throws IOException {
        Path mainJava = moduleRoot.resolve("src").resolve("main").resolve("java");
        assertThat(mainJava).as("main sources of %s", moduleRoot.getFileName()).exists();
        StringBuilder sources = new StringBuilder();
        try (Stream<Path> walk = Files.walk(mainJava)) {
            List<Path> javaFiles = walk.filter(path -> path.toString().endsWith(".java")).toList();
            for (Path javaFile : javaFiles) {
                sources.append(Files.readString(javaFile, StandardCharsets.UTF_8)).append('\n');
            }
        }
        return sources.toString();
    }
}
