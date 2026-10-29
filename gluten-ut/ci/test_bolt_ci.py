# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("bolt_ci", ROOT / ".github/scripts/bolt_ci.py")
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


class BoltCiTest(unittest.TestCase):
    def test_unpatched_bolt_source_is_reusable_and_rejects_later_edits(self):
        script = (ROOT / "dev/ci-bolt-build.sh").read_text()
        snippet = script.split("<<'PY_COMPAT'\n", 1)[1].split("\nPY_COMPAT", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original, gluten = root / "bolt", root / "gluten"
            original.mkdir()
            reports = gluten / "bolt-ci-reports/native"
            reports.mkdir(parents=True)
            subprocess.run(["git", "init", "-q", str(original)], check=True)
            source_file = original / "source.txt"
            source_file.write_text("original\n")
            subprocess.run(["git", "-C", str(original), "add", "source.txt"], check=True)
            subprocess.run(["git", "-C", str(original), "-c", "user.name=Test",
                            "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"],
                           check=True)
            command = [sys.executable, "-c", snippet, str(original), str(gluten)]
            for _ in range(2):
                result = subprocess.run(command, text=True, capture_output=True)
                self.assertEqual(0, result.returncode, result.stderr)
            proof = json.loads((reports / "bolt-compat-source.json").read_text())
            self.assertEqual({}, proof["patches"])
            self.assertEqual({}, proof["changed_files"])
            isolated = Path((reports / "effective-bolt-source.txt").read_text().strip())
            self.assertNotEqual(original, isolated)
            (isolated / "source.txt").write_text("modified\n")
            rejected = subprocess.run(command, text=True, capture_output=True)
            self.assertNotEqual(0, rejected.returncode)
            self.assertIn("does not match its pinned patches", rejected.stderr)
            self.assertEqual("original\n", source_file.read_text())

    def _run_script_fixture(self, script):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            activation = root / "cpp/build/Release/generators/conanrun.sh"
            activation.parent.mkdir(parents=True)
            activation.write_text("")
            (root / "cpp/build/bolt-ci-s3-enabled.txt").write_text("False\n")
            java = root / "jdk/bin/java"
            java.parent.mkdir(parents=True)
            java.write_text('#!/bin/sh\nprintf \'openjdk version "17.0.fixture"\\n\' >&2\n')
            java.chmod(0o755)
            workflow = root / "reference.yml"
            workflow.write_text(ci.yaml.safe_dump({"jobs": {"script-fixture": {"steps": [
                {"name": "Run script fixture", "run": script}]}}}))
            case = {"job": "script-fixture", "java": "17", "scala": "2.12", "matrix": {}}
            try:
                ci.reference.cache_clear()
                with mock.patch.object(ci, "ROOT", root), mock.patch.object(ci, "REFERENCE", workflow), \
                        mock.patch.dict(os.environ, JAVA_HOME=str(java.parent.parent)):
                    failed = ci.run_case(case, root / "reports", False)
                return failed, (root / "reports/step-0.log").read_text()
            finally:
                ci.reference.cache_clear()

    def test_bolt_ci_marks_its_launcher_environment(self):
        failed, log = self._run_script_fixture(
            'printf "BOLT_ENV:%s\\n" "${GLUTEN_BOLT_CI:-missing}"\n')
        self.assertFalse(failed, log)
        self.assertIn("BOLT_ENV:true", log)

    def test_spark_run_preserves_backend_report_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backend = root / "backends-bolt/target/surefire-reports/TEST-backend.xml"
            spark = root / "gluten-ut/spark35/target/surefire-reports/TEST-spark.xml"
            for report in (backend, spark):
                report.parent.mkdir(parents=True)
                report.write_text("<testsuite/>")
            case = {"job": "spark-test-spark35", "spark": "3.5"}
            self.assertEqual({backend, spark}, set(ci.fresh_reports(root, case, 0, spark_only=False)))
            self.assertEqual([spark], ci.fresh_reports(root, case, 0, spark_only=True))
            spark.unlink()
            self.assertEqual([], ci.fresh_reports(root, case, 0, spark_only=True))

    def test_bolt_ci_supplies_its_exact_ut_blacklist(self):
        failed, log = self._run_script_fixture(
            'test "${GLUTEN_BOLT_UT_BLACKLIST:-missing}" = '
            '"$GITHUB_WORKSPACE/.github/bolt-ut-blacklist.json"\n')
        self.assertFalse(failed, log)

    def test_scalatest_child_skips_do_not_count_as_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "TEST-fixture.xml"
            report.write_text('<testsuite tests="2" failures="0" errors="0">'
                              '<testcase name="ignored"><skipped/></testcase>'
                              '<testcase name="canceled"><skipped/></testcase></testsuite>')
            with self.assertRaises(ValueError):
                ci.verify_reports([report])
            report.write_text('<testsuite tests="2" failures="0" errors="0">'
                              '<testcase name="executed"/>'
                              '<testcase name="ignored"><skipped/></testcase></testsuite>')
            self.assertEqual(1, ci.verify_reports([report]))

    def test_compile_only_step_does_not_require_runtime_xml(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            activation = root / "cpp/build/Release/generators/conanrun.sh"
            activation.parent.mkdir(parents=True)
            activation.write_text("")
            (root / "cpp/build/bolt-ci-s3-enabled.txt").write_text("False\n")
            java = root / "jdk/bin/java"
            java.parent.mkdir(parents=True)
            java.write_text('#!/bin/sh\nprintf \'openjdk version "17.0.fixture"\\n\' >&2\n')
            java.chmod(0o755)
            maven = root / "build/mvn"
            maven.parent.mkdir()
            maven.write_text('#!/bin/sh\ntouch compile-executed\n')
            maven.chmod(0o755)
            workflow = root / "reference.yml"
            workflow.write_text(
                'jobs:\n  build-fast-build-test:\n    steps:\n'
                '      - name: Build compile fixture\n'
                '        run: $MVN_CMD clean test-compile -Pbackends-velox\n')
            case = {"job": "build-fast-build-test", "java": "17", "scala": "2.13", "matrix": {}}
            try:
                ci.reference.cache_clear()
                with mock.patch.object(ci, "ROOT", root), mock.patch.object(ci, "REFERENCE", workflow), \
                        mock.patch.dict(os.environ, JAVA_HOME=str(java.parent.parent)):
                    failed = ci.run_case(case, root / "reports", False)
                self.assertTrue((root / "compile-executed").is_file())
                self.assertFalse(failed, (root / "reports/status.json").read_text())
            finally:
                ci.reference.cache_clear()

    def test_step_environment_files_are_applied_to_following_steps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            activation = root / "cpp/build/Release/generators/conanrun.sh"
            activation.parent.mkdir(parents=True)
            activation.write_text("")
            (root / "cpp/build/bolt-ci-s3-enabled.txt").write_text("False\n")
            tool = root / "added-path/reference-tool"
            tool.parent.mkdir()
            tool.write_text('#!/bin/sh\nprintf "environment-carried\\n"\n')
            tool.chmod(0o755)
            java = root / "jdk/bin/java"
            java.parent.mkdir(parents=True)
            java.write_text('#!/bin/sh\nprintf \'openjdk version "17.0.fixture"\\n\' >&2\n')
            java.chmod(0o755)
            workflow = root / "reference.yml"
            workflow.write_text(
                'jobs:\n  environment-replay:\n    steps:\n'
                '      - name: Build environment fixture\n        run: |\n'
                '          echo "REFERENCE_ENV=ready" >> "$GITHUB_ENV"\n'
                f'          echo "{tool.parent}" >> "$GITHUB_PATH"\n'
                '      - name: Run environment fixture\n        run: |\n'
                '          test "$REFERENCE_ENV" = ready\n'
                '          reference-tool\n')
            case = {"job": "environment-replay", "java": "17", "scala": "2.12", "matrix": {}}
            try:
                ci.reference.cache_clear()
                with mock.patch.object(ci, "ROOT", root), mock.patch.object(ci, "REFERENCE", workflow), \
                        mock.patch.dict(os.environ, JAVA_HOME=str(java.parent.parent)):
                    failed = ci.run_case(case, root / "reports", False)
                self.assertFalse(failed, (root / "reports/status.json").read_text())
                self.assertIn('environment-carried', (root / "reports/step-1.log").read_text())
            finally:
                ci.reference.cache_clear()

    def test_native_commands_execute_from_conan_release_layout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            build = root / "cpp/build/Release"
            benchmark = build / "bolt/benchmarks/generic_benchmark"
            benchmark.parent.mkdir(parents=True)
            marker = root / "native-test-executed"
            (build / "CTestTestfile.cmake").write_text(
                f'add_test(layout "{sys.executable}" "-c" '
                f'"from pathlib import Path; Path(\'{marker}\').touch()")\n')
            benchmark.write_text('#!/bin/sh\nprintf "benchmark-executed\\n"\n')
            benchmark.chmod(0o755)
            commands = ("cd ./cpp/build && ctest -V",
                        "./cpp/build/velox/benchmarks/generic_benchmark --threads 1 --iterations 1")
            for command in commands:
                result = subprocess.run(["bash", "-euc", ci.adapt(command)], cwd=root,
                                        capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                if "ctest" in command:
                    self.assertTrue(marker.exists(), result.stdout)
                else:
                    self.assertIn("benchmark-executed", result.stdout)

    def test_spark_matrix_covers_every_reference_group(self):
        cases = ci.plan(False)
        spark = [case for case in cases if case["job"].startswith("spark-test-")]
        self.assertEqual(28, len(spark))
        self.assertEqual(11, len({case["job"] for case in spark}))
        self.assertEqual(3, sum(case["job"] == "spark-test-spark42" for case in spark))
        self.assertEqual(2, sum(case["job"] == "spark-test-spark42-slow" for case in spark))
        for case in spark:
            script = ci.test_steps(case)[0]["script"]
            self.assertIn("-Pbackends-bolt", script)
            self.assertNotIn("-Pbackends-velox", script)
            self.assertNotIn("failure.ignore", script)
            self.assertNotIn(" -pl ", script)
            self.assertIn("clean test", script)

    def test_python_setup_preserves_reference_pyspark_for_spark4(self):
        cases = ci.plan(False)
        for spark, python in [("4.0", "3.9"), ("4.1", "3.11")]:
            case = next(case for case in cases
                        if case["job"].startswith("spark-test-") and case["spark"] == spark
                        and case["matrix"].get("group") == 1)
            self.assertEqual(python, case.get("python"))
            self.assertIn("pyspark==3.5.5", case.get("python_packages", "").split())
            self.assertNotIn("pyspark==" + spark, case["python_packages"])

    def test_compile_only_cases_do_not_require_runtime_downloads(self):
        for case in ci.plan(False):
            if case["job"] in ("build-fast-build-test", "build-test-spark42"):
                self.assertFalse(case.get("runtime_setup", True))
                self.assertEqual("", case.get("python_packages"))

    def test_ut_plan_defers_only_known_tpc_jobs_and_keeps_native_and_build_cases(self):
        deferred = {
            "tpc-test-ubuntu", "tpc-test-centos8", "tpc-test-ubuntu-oom",
            "tpc-test-ubuntu-randomkill", "tpc-test-centos8-uniffle",
            "tpc-test-ubuntu-2204-celeborn",
        }
        for full in (False, True):
            cases = ci.plan(full)
            jobs = {case["job"] for case in cases}
            self.assertFalse(jobs & deferred)
            self.assertEqual(set(ci.reference()["jobs"]) - deferred - set(ci.INFRA_JOBS), jobs)
            self.assertEqual(31, len(cases))
            self.assertIn("cpp-test-udf-test", jobs)
            self.assertIn("build-test-spark42", jobs)
            native = next(case for case in cases if case["job"] == "cpp-test-udf-test")
            self.assertEqual([
                "Run CPP unit test", "Run CPP benchmark test", "Run UDF test",
                "Run CPP benchmark test for Spark 4.0",
            ], [step["name"] for step in ci.test_steps(native)])

    def test_unknown_reference_job_is_rejected_even_when_change_flags_disable_it(self):
        workflow = ci.reference()
        flags = dict.fromkeys(("cpp", "java", "shims34", "shims35", "shims40",
                               "shims41", "shims42", "tools_it", "full_run"), "false")
        for name in ("tpc-test-new-coverage", "spark-test-new-coverage"):
            with self.subTest(job=name):
                modified = {"jobs": {**workflow["jobs"], name: {
                    "if": "needs.detect-changes.outputs.java == 'true'",
                    "steps": [{"name": "Run future test", "run": "true"}],
                }}}
                with mock.patch.object(ci, "reference", return_value=modified):
                    with self.assertRaisesRegex(ValueError, "requires review"):
                        ci.plan(False, flags)

    def test_plan_publishes_deferred_coverage_without_marking_it_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([
                sys.executable, str(ROOT / ".github/scripts/bolt_ci.py"),
                "plan", "--reports", directory,
            ], capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stderr)
            matrix = json.loads(result.stdout)
            scope = Path(directory) / "scope.json"
            self.assertTrue(scope.is_file(), "The published matrix must include deferred coverage evidence")
            report = json.loads(scope.read_text())
            self.assertEqual(matrix["include"], report["retained_cases"])
            self.assertEqual({
                "tpc-test-ubuntu", "tpc-test-centos8", "tpc-test-ubuntu-oom",
                "tpc-test-ubuntu-randomkill", "tpc-test-centos8-uniffle",
                "tpc-test-ubuntu-2204-celeborn",
            }, {item["job"] for item in report["deferred_jobs"]})
            for item in report["deferred_jobs"]:
                self.assertEqual("deferred", item["status"])
                self.assertTrue(item["reason"])
            self.assertEqual(64, len(report["reference_sha256"]))

    def test_spark_lance_profiles_are_preserved(self):
        for job in ("spark-test-spark35", "spark-test-spark40"):
            case = next(case for case in ci.plan(False)
                        if case["job"] == job and case["matrix"]["group"] == 1)
            self.assertIn("-Plance -Plance-test", ci.test_steps(case)[0]["script"])

    def test_missing_matrix_value_fails_instead_of_leaving_expression(self):
        with self.assertRaises(ValueError):
            ci.render("${{ matrix.unknown }}", {})

    def test_unknown_reference_run_step_is_rejected(self):
        with self.assertRaises(ValueError):
            ci.step_role({"name": "New unexplained test", "run": "some-test"})

    def test_empty_scalatest_report_is_not_a_pass(self):
        with self.assertRaises(ValueError):
            ci.verify_reports([])

    def test_new_bolt_inputs_trigger_the_reference_job_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_git = root / "git"
            fake_git.write_text('#!/bin/sh\nprintf "%s\\n" "$BOLT_CHANGED_PATH"\n')
            fake_git.chmod(0o755)
            for changed, required in ((".github/bolt-ut-blacklist.json", "java"),
                                      (".github/bolt-ci-patches/001.patch", "cpp")):
                with self.subTest(changed=changed):
                    output = root / "outputs"
                    output.write_text("")
                    env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                               BOLT_CHANGED_PATH=changed, GITHUB_OUTPUT=str(output),
                               GITHUB_EVENT_NAME="pull_request", BOLT_CI_BASE_SHA="base",
                               GITHUB_SHA="head")
                    with mock.patch.object(ci, "ROOT", root), \
                            mock.patch.object(sys, "argv", ["bolt_ci.py", "detect"]), \
                            mock.patch.dict(os.environ, env, clear=True):
                        self.assertEqual(0, ci.main())
                    flags = dict(line.split("=", 1) for line in output.read_text().splitlines())
                    self.assertEqual("true", flags[required])
                    self.assertEqual("false", flags["full_run"])

    def test_java_only_change_does_not_enable_native_chaos_jobs(self):
        flags = dict.fromkeys(("cpp", "java", "shims34", "shims35", "shims40",
                               "shims41", "shims42", "tools_it", "full_run"), "false")
        flags["java"] = "true"
        jobs = {case["job"] for case in ci.plan(False, flags)}
        self.assertNotIn("tpc-test-ubuntu-oom", jobs)
        self.assertNotIn("tpc-test-ubuntu-randomkill", jobs)
        self.assertNotIn("cpp-test-udf-test", jobs)
        self.assertIn("spark-test-spark41", jobs)

    def test_ctest_failed_fixture_cannot_be_masked_by_skip(self):
        log = ('1/140 Test #140: MyUdfTest.hivestringstring ...***Skipped\n'
               '140: [  FAILED  ] MyUdfTest: SetUpTestSuite or TearDownTestSuite\n'
               '100% tests passed, 0 tests failed out of 140')
        with self.assertRaises(ValueError):
            ci.verify_execution("ctest -V", log)

    def test_ctest_all_skipped_is_not_execution(self):
        with self.assertRaises(ValueError):
            ci.verify_execution("ctest -V", "1/1 Test #1: skipped ...***Skipped 0.01 sec")
        ci.verify_execution("ctest -V", "1/2 Test #1: real ... Passed 0.01 sec\n"
                            "2/2 Test #2: skipped ...***Skipped 0.01 sec")

    def test_ctest_no_tests_is_rejected(self):
        with self.assertRaises(ValueError):
            ci.verify_execution("ctest -V", "No tests were found!!!")

    def test_native_job_preserves_jdk8_then_jdk17_transition(self):
        case = next(case for case in ci.plan(False) if case["job"] == "cpp-test-udf-test")
        self.assertEqual("8", case["java"])
        steps = ci.test_steps(case)
        udf = next(step for step in steps if step["name"] == "Run UDF test")
        self.assertIn('export JAVA_HOME="$JAVA17_HOME"', udf["script"])

    def test_backend_reports_cannot_mask_empty_spark_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            backend = root / "backends-bolt/target/surefire-reports/TEST-backend.xml"
            spark = root / "gluten-ut/spark35/target/surefire-reports/TEST-spark.xml"
            for path, tests, skipped in [(backend, 5, 0), (spark, 3, 3)]:
                path.parent.mkdir(parents=True)
                path.write_text(f'<testsuite tests="{tests}" skipped="{skipped}"/>')
            case = {"job": "spark-test-spark35", "spark": "3.5"}
            with self.assertRaises(ValueError):
                ci.verify_reports(ci.fresh_reports(root, case, 0))


if __name__ == "__main__":
    unittest.main()
