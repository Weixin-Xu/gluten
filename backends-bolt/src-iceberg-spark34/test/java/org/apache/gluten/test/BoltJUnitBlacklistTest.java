/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.test;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/** Exercises exact invocation selection with the real JUnit engines and blacklist policy. */
public class BoltJUnitBlacklistTest {
  private static Path blacklist;
  private static final AtomicInteger bodies = new AtomicInteger();

  private static void select(String suite, String test) throws Exception {
    blacklist = Files.createTempFile("bolt-junit-blacklist", ".json");
    String json =
        "{\"entries\":[{\"spark\":\"3.4\",\"scala\":\"2.12\",\"suite\":\""
            + suite
            + "\",\"test\":\""
            + test
            + "\",\"reason\":\"regression fixture\",\"evidence\":\"fixture\"}]}";
    Files.write(blacklist, json.getBytes(StandardCharsets.UTF_8));
    bodies.set(0);
  }

  private static boolean selected(String suite, String test) {
    return BoltTestBlacklist.shouldSkip(
        true,
        () -> new scala.Some<>("bolt"),
        () -> BoltTestBlacklist.load(blacklist),
        "3.4",
        "2.12",
        suite,
        test);
  }

  private static TestExecutionSummary runJupiter(Class<?> fixture) {
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    LauncherFactory.create()
        .execute(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(fixture))
                .build(),
            listener);
    return listener.getSummary();
  }

  @Test
  public void jupiterSkipsOnlyTheExactTemplateInvocation() throws Exception {
    select(JupiterFixture.class.getName(), "row()[2]");
    try {
      TestExecutionSummary summary = runJupiter(JupiterFixture.class);
      assertEquals(1, summary.getTestsSkippedCount());
      assertEquals(5, summary.getTestsSucceededCount());
      assertEquals(0, summary.getTestsFailedCount());
      assertEquals(5, bodies.get());
    } finally {
      Files.deleteIfExists(blacklist);
    }
  }

  @Test
  public void jupiterMatchesTheExactTypedTemplateSignature() throws Exception {
    select(TypedJupiterFixture.class.getName(), "row(int, String)[2]");
    try {
      TestExecutionSummary summary = runJupiter(TypedJupiterFixture.class);
      assertEquals(1, summary.getTestsSkippedCount());
      assertEquals(2, summary.getTestsSucceededCount());
      assertEquals(0, summary.getTestsFailedCount());
      assertEquals(2, bodies.get());
    } finally {
      Files.deleteIfExists(blacklist);
    }
  }

  @Test
  public void jupiterKeepsBeforeAllFailureVisible() throws Exception {
    select(BrokenJupiterFixture.class.getName(), "row");
    try {
      TestExecutionSummary summary = runJupiter(BrokenJupiterFixture.class);
      assertEquals(1, summary.getContainersFailedCount());
      assertEquals(0, summary.getTestsSkippedCount());
      assertEquals(0, bodies.get());
    } finally {
      Files.deleteIfExists(blacklist);
    }
  }

  @Test
  public void junit4SkipsOnlyTheExactParameterizedDescription() throws Exception {
    select(JUnit4Fixture.class.getName(), "row[1: value = 20]");
    try {
      Result result = JUnitCore.runClasses(JUnit4Fixture.class);
      assertEquals(0, result.getFailureCount());
      assertEquals(1, result.getAssumptionFailureCount());
      assertEquals(6, result.getRunCount());
      assertEquals(5, bodies.get());
    } finally {
      Files.deleteIfExists(blacklist);
    }
  }

  @Test
  public void junit4KeepsBeforeClassFailureVisible() throws Exception {
    select(BrokenJUnit4Fixture.class.getName(), "row");
    try {
      Result result = JUnitCore.runClasses(BrokenJUnit4Fixture.class);
      assertEquals(1, result.getFailureCount());
      assertEquals(0, result.getAssumptionFailureCount());
      assertEquals(0, bodies.get());
    } finally {
      Files.deleteIfExists(blacklist);
    }
  }

  public static class PolicyCondition extends BoltJUnit5BlacklistCondition {
    @Override
    protected boolean isBlacklisted(String suite, String test) {
      return selected(suite, test);
    }
  }

  public static class PolicyRule extends BoltJUnit4BlacklistRule {
    @Override
    protected boolean isBlacklisted(String suite, String test) {
      return selected(suite, test);
    }
  }

  public static class ThreeRows implements TestTemplateInvocationContextProvider {
    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
      return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
        ExtensionContext context) {
      return Stream.of(10, 20, 30)
          .map(
              value ->
                  new TestTemplateInvocationContext() {
                    @Override
                    public String getDisplayName(int index) {
                      return "catalog parameter = " + value;
                    }
                  });
    }
  }

  public static class TypedRows extends ThreeRows {
    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
        ExtensionContext context) {
      return Stream.of(10, 20, 30)
          .map(
              value ->
                  new TestTemplateInvocationContext() {
                    @Override
                    public List<Extension> getAdditionalExtensions() {
                      return Collections.singletonList(
                          new ParameterResolver() {
                            @Override
                            public boolean supportsParameter(
                                ParameterContext parameter, ExtensionContext extension) {
                              return true;
                            }

                            @Override
                            public Object resolveParameter(
                                ParameterContext parameter, ExtensionContext extension) {
                              return parameter.getIndex() == 0 ? value : "catalog";
                            }
                          });
                    }
                  });
    }
  }

  @ExtendWith({PolicyCondition.class, TypedRows.class})
  public static class TypedJupiterFixture {
    @TestTemplate
    void row(int value, String catalog) {
      bodies.incrementAndGet();
    }
  }

  @ExtendWith({PolicyCondition.class, ThreeRows.class})
  public static class JupiterFixture {
    @TestTemplate
    void row() {
      bodies.incrementAndGet();
    }

    @TestTemplate
    void rowWithSuffix() {
      bodies.incrementAndGet();
    }
  }

  @ExtendWith(PolicyCondition.class)
  public static class BrokenJupiterFixture {
    @BeforeAll
    static void failSetup() {
      throw new IllegalStateException("fixture setup must remain visible");
    }

    @org.junit.jupiter.api.Test
    void row() {
      bodies.incrementAndGet();
    }
  }

  @RunWith(Parameterized.class)
  public static class JUnit4Fixture {
    @Parameterized.Parameters(name = "{index}: value = {0}")
    public static Collection<Object[]> rows() {
      return Arrays.asList(new Object[][] {{10}, {20}, {30}});
    }

    public JUnit4Fixture(int value) {}

    @Rule public final PolicyRule blacklistRule = new PolicyRule();

    @Test
    public void row() {
      bodies.incrementAndGet();
    }

    @Test
    public void rowWithSuffix() {
      bodies.incrementAndGet();
    }
  }

  public static class BrokenJUnit4Fixture {
    @Rule public final PolicyRule blacklistRule = new PolicyRule();

    @BeforeClass
    public static void failSetup() {
      throw new IllegalStateException("fixture setup must remain visible");
    }

    @Test
    public void row() {
      bodies.incrementAndGet();
    }
  }
}
