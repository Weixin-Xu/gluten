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

import org.apache.gluten.source.TestGlutenIcebergSourceHiveTables;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Runs the real Hive fixture lifecycle without performing an Iceberg data scan. */
public class IcebergHiveFixtureCompatibilityTest {
  @Test
  public void vintageRestoresTheUpstreamNamespaceAndTableCleanup() {
    Fixture.setupCount = 0;
    Fixture.bodyCount = 0;
    boolean[] tableDropped = {false};
    JUnitCore runner = new JUnitCore();
    runner.addListener(
        new RunListener() {
          @Override
          public void testFinished(Description description) {
            tableDropped[0] = !Fixture.tableExists();
          }
        });
    Result result = runner.run(Request.method(Fixture.class, "verifyFixture"));
    assertEquals(result.getFailures().toString(), 0, result.getFailureCount());
    assertEquals(1, result.getRunCount());
    assertEquals(0, result.getAssumptionFailureCount());
    assertEquals(1, Fixture.setupCount);
    assertEquals(1, Fixture.bodyCount);
    assertTrue(
        "Upstream cleanup must remove the fixture table before the next test", tableDropped[0]);
  }

  public static class Fixture extends TestGlutenIcebergSourceHiveTables {
    private static final TableIdentifier TABLE = TableIdentifier.of("db", "fixture_compatibility");
    private static int setupCount;
    private static int bodyCount;

    @BeforeClass
    public static void countFixtureSetup() {
      setupCount++;
    }

    @Test
    public void verifyFixture() {
      bodyCount++;
      assertTrue(
          "Inherited Iceberg setup must create db", catalog.namespaceExists(Namespace.of("db")));
      assertFalse(
          "Each invocation must start with a clean fixture table", catalog.tableExists(TABLE));
      createTable(
          TABLE,
          new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
          PartitionSpec.unpartitioned(),
          Collections.emptyMap());
      assertTrue(catalog.tableExists(TABLE));
    }

    private static boolean tableExists() {
      return catalog.tableExists(TABLE);
    }
  }
}
