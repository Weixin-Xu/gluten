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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Applies the Bolt CI blacklist to individual Jupiter test invocations. */
public class BoltJUnit5BlacklistCondition implements ExecutionCondition {
  private static final Pattern TEMPLATE_INVOCATION =
      Pattern.compile("\\[test-template-invocation:#(\\d+)\\]$");

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    // Class and template containers remain active, including their fixture lifecycle.
    if (!context.getTestMethod().isPresent() || !context.getTestClass().isPresent()) {
      return ConditionEvaluationResult.enabled("Not an individual test invocation");
    }
    Method method = context.getRequiredTestMethod();
    Matcher invocation = TEMPLATE_INVOCATION.matcher(context.getUniqueId());
    String test = method.getName();
    if (invocation.find()) {
      String parameters =
          Arrays.stream(method.getParameterTypes())
              .map(Class::getSimpleName)
              .collect(Collectors.joining(", "));
      test += "(" + parameters + ")[" + invocation.group(1) + "]";
    } else if (context.getUniqueId().contains("[test-template:")) {
      return ConditionEvaluationResult.enabled("Not an individual template invocation");
    }
    if (isBlacklisted(context.getRequiredTestClass().getName(), test)) {
      return ConditionEvaluationResult.disabled("Known Bolt UT failure: " + test);
    }
    return ConditionEvaluationResult.enabled("Test is not selected by the Bolt CI blacklist");
  }

  protected boolean isBlacklisted(String suite, String test) {
    return BoltTestBlacklistSupport.isBlacklisted(suite, test);
  }
}
