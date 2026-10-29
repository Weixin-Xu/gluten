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

"""Run Gluten's reference x86 test commands against the Bolt backend.

The reference YAML is read from this checkout, never from another branch.
Platform setup is separate; retained UT profiles, tags and matrices are inherited.
TPC query jobs and plan stability suites are explicitly deferred in this phase.
"""

import argparse
import functools
import hashlib
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / ".github/workflows/velox_backend_x86.yml"
INFRA_JOBS = ("detect-changes", "build-native-lib-centos-8", "build-cudf-centos-9")
RETAINED_JOBS = (
    "spark-test-spark34", "spark-test-spark34-slow",
    "spark-test-spark35", "spark-test-spark35-scala213", "spark-test-spark35-slow",
    "spark-test-spark40", "spark-test-spark40-slow",
    "spark-test-spark41", "spark-test-spark41-slow",
    "spark-test-spark42", "spark-test-spark42-slow",
    "cpp-test-udf-test", "build-fast-build-test", "build-test-spark42",
)
DEFERRED_JOBS = {
    "tpc-test-ubuntu": "TPC-H/TPC-DS query comparisons, including HDFS/S3 integration",
    "tpc-test-centos8": "TPC-H/TPC-DS query comparisons on CentOS",
    "tpc-test-ubuntu-oom": "TPC-DS low-memory and memory-isolation query coverage",
    "tpc-test-ubuntu-randomkill": "TPC-DS random task-kill query coverage",
    "tpc-test-centos8-uniffle": "TPC-H/TPC-DS query coverage with Uniffle shuffle",
    "tpc-test-ubuntu-2204-celeborn": "TPC-H/TPC-DS query coverage with Celeborn shuffle",
}
SETUP_STEPS = {
    "Prepare spark.test.home for Spark 3.4.4 (other tests)",
    "Setup tzdata", "Setup java", "Setup java and maven", "Set environment variables",
    "Update mirror list", "Maximize build disk space", "Prepare",
    "Install Stash restore dependencies", "Build Gluten native libraries", "Check Clang Tidy",
}


@functools.lru_cache(maxsize=1)
def reference():
    return yaml.safe_load(REFERENCE.read_text())


def values(value, full):
    if isinstance(value, list):
        return value
    alternatives = re.findall(r"'(\[[^']*\])'", value)
    if len(alternatives) != 2:
        raise ValueError(f"Unsupported reference matrix expression: {value}")
    return json.loads(alternatives[0 if full else 1])


def selected_job(condition, flags):
    if flags is None:
        return True
    selected = False
    for clause in condition.split("||"):
        match = re.fullmatch(r"\s*needs\.detect-changes\.outputs\.(\w+)\s*==\s*'true'\s*", clause)
        if not match or match[1] not in flags:
            raise ValueError(f"Unsupported reference job condition: {condition}")
        selected = selected or flags[match[1]] == "true"
    return selected


def plan(full, flags=None):
    jobs = reference()["jobs"]
    unknown = set(jobs) - set(INFRA_JOBS) - set(RETAINED_JOBS) - set(DEFERRED_JOBS)
    if unknown:
        raise ValueError(f"New reference job requires review: {sorted(unknown)}")
    cases = []
    for job, config in jobs.items():
        if job in INFRA_JOBS or job in DEFERRED_JOBS:
            continue
        if not selected_job(config.get("if", ""), flags):
            continue
        matrix = config.get("strategy", {}).get("matrix", {})
        keys = [key for key in matrix if key not in ("exclude", "include")]
        if "include" in matrix:
            raise ValueError(f"New include matrix requires review: {job}")
        for combination in itertools.product(*(values(matrix[key], full) for key in keys)):
            parameters = dict(zip(keys, combination))
            if any(all(parameters.get(key) == value for key, value in excluded.items())
                   for excluded in matrix.get("exclude", [])):
                continue
            suffix = "-".join(str(value).replace(":", "-") for value in combination)
            case = {"id": job + ("-" + suffix if suffix else ""), "job": job,
                    "matrix": parameters, "full": full}
            spark = parameters.get("spark", "")
            if not spark:
                match = re.search(r"spark(34|35|40|41|42)", job)
                spark = "spark-" + match[1][0] + "." + match[1][1] if match else "spark-3.5"
                if job == "build-fast-build-test":
                    spark = "spark-4.0"
            case["spark"] = spark.removeprefix("spark-")
            case["scala"] = "2.13" if "scala213" in job or case["spark"].startswith("4") else "2.12"
            default_java = "java-8" if job == "cpp-test-udf-test" else "java-17"
            case["java"] = parameters.get("java", default_java).removeprefix("java-")
            case["source_container"] = render(config.get("container", "ubuntu-22.04"), parameters)
            setup = "\n".join(step.get("run", "") for step in config["steps"]
                              if step_role(step) == "setup")
            python = re.search(r"alternatives --set python3 /usr/bin/python(3\.\d+)", setup)
            case["python"] = python[1] if python else "3.10"
            packages = re.findall(r"pip3? install ([^\n]+)", setup)
            case["python_packages"] = " ".join(
                line.removesuffix("\\").strip().removesuffix("&&").strip() for line in packages)
            case["runtime_setup"] = job not in ("build-fast-build-test", "build-test-spark42")
            cases.append(case)
    return cases


def render(script, parameters):
    def substitute(match):
        key = match[1]
        if key not in parameters:
            raise ValueError(f"Unknown matrix value: {key}")
        return str(parameters[key])
    result = re.sub(r"\$\{\{\s*matrix\.(\w+)\s*\}\}", substitute, script)
    if "${{" in result:
        raise ValueError("Unresolved workflow expression in test command")
    return result


def adapt(script):
    # These are backend selectors, not exclusions or query rewrites.
    return (script.replace("backends-velox", "backends-bolt")
            .replace("cd ./cpp/build && ctest", "cd ./cpp/build/Release && ctest")
            .replace("cpp/build/velox/benchmarks", "cpp/build/Release/bolt/benchmarks"))


def step_role(step):
    if "run" not in step:
        return "artifact"
    name = step.get("name", "")
    if name in SETUP_STEPS:
        return "setup"
    if name.startswith(("Build ", "Run ")):
        return "test"
    raise ValueError(f"Unclassified reference step: {name}")


def condition_matches(condition, parameters):
    if not condition:
        return True
    for clause in condition.split("&&"):
        match = re.fullmatch(r"\s*matrix\.(\w+)\s*==\s*'([^']+)'\s*", clause)
        if not match:
            raise ValueError(f"Unsupported test condition: {condition}")
        if parameters.get(match[1]) != match[2]:
            return False
    return True


def test_steps(case):
    steps = []
    for step in reference()["jobs"][case["job"]]["steps"]:
        if step_role(step) != "test" or not condition_matches(step.get("if"), case["matrix"]):
            continue
        original = render(step["run"], case["matrix"])
        script = adapt(original)
        # JDK is supplied by setup-java for this case. Do not install another one.
        script = re.sub(r"^\s*yum install -y java-17-openjdk-devel\s*$", "", script, flags=re.M)
        java_home = '$JAVA17_HOME' if case["job"] == "cpp-test-udf-test" else '$JAVA_HOME'
        script = re.sub(r"export JAVA_HOME=/usr/lib/jvm/[^\s]+", 'export JAVA_HOME="' + java_home + '"', script)
        steps.append({"name": render(step["name"], case["matrix"]),
                      "original": original, "script": script})
    if not steps:
        raise ValueError(f"Reference job has no test commands: {case['job']}")
    return steps


def verify_reports(paths):
    total = 0
    failures = 0
    for path in paths:
        root = ET.parse(path).getroot()
        suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
        for suite in suites:
            # ScalaTest reports skipped children without a suite skipped attribute.
            child_skips = sum(test.find("skipped") is not None for test in suite.findall("testcase"))
            skipped = max(int(suite.get("skipped", "0")), child_skips)
            total += int(suite.get("tests", "0")) - skipped
            failures += int(suite.get("failures", "0")) + int(suite.get("errors", "0"))
    if total <= 0:
        raise ValueError("No executed tests in the fresh XML reports")
    if failures:
        raise ValueError(f"Test reports contain {failures} failures/errors")
    return total


def verify_execution(script, log):
    if "ctest -V" in script and not re.search(r"\d+/[1-9]\d*\s+Test\s+#\d+:.*\bPassed\b", log):
        raise ValueError("CTest did not execute any native tests")
    if "ctest -V" in script and re.search(r"\[\s*FAILED\s*\]", log):
        raise ValueError("Native GTest failure was masked by CTest skip detection")


def fresh_reports(root, case, start, spark_only=True):
    # Backend UT reports cannot stand in for an empty Spark UT group.
    scope = root
    if spark_only and case["job"].startswith("spark-test-"):
        scope = root / "gluten-ut" / ("spark" + case["spark"].replace(".", ""))
    return [path for path in scope.glob("**/surefire-reports/TEST-*.xml")
            if path.stat().st_mtime >= start]


def run_case(case, reports, dry_run):
    reports.mkdir(parents=True, exist_ok=True)
    steps = test_steps(case)
    (reports / "commands.json").write_text(json.dumps({"case": case, "steps": steps}, indent=2) + "\n")
    config = reference()["jobs"][case["job"]]
    env = {**os.environ, "GITHUB_WORKSPACE": str(ROOT), "MVN_CMD": "build/mvn -ntp",
           "WGET_CMD": "wget -nv", "GLUTEN_TEST_MATCH_VELOX": "true",
           "SPARK_SCALA_VERSION": case["scala"], "SPARK_ANSI_SQL_MODE": "false",
           "GLUTEN_BOLT_CI": "true",
           "GLUTEN_BOLT_UT_BLACKLIST": str(ROOT / ".github/bolt-ut-blacklist.json")}
    environment_file = reports / "github-env.txt"
    path_file = reports / "github-path.txt"
    environment_file.write_text("")
    path_file.write_text("")
    env.update(GITHUB_ENV=str(environment_file), GITHUB_PATH=str(path_file))
    environment_offset = path_offset = 0
    runtime = ROOT / "cpp/build/releases/third-party-runtime"
    features = ROOT / "cpp/build/bolt-ci-s3-enabled.txt"
    if not dry_run and not features.is_file():
        raise ValueError("Missing native artifact S3 feature state; build native first")
    s3_enabled = features.read_text().strip() if features.is_file() else "False"
    if s3_enabled not in ("True", "False"):
        raise ValueError("Invalid S3 feature state in the native artifact")
    env["BOLT_CI_ENABLE_S3"] = s3_enabled
    if not dry_run:
        java = Path(env.get("JAVA_HOME", "")) / "bin/java"
        version = subprocess.run([str(java), "-version"], capture_output=True, text=True, check=True)
        expected = "1.8" if case["java"] == "8" else case["java"]
        if not re.search(r'version "' + re.escape(expected) + r'[.\"]', version.stderr + version.stdout):
            raise ValueError(f"Environment requires reference JDK {case['java']}")
        if case["job"] == "cpp-test-udf-test" and not env.get("JAVA17_HOME"):
            raise ValueError("The native/UDF reference job also requires JAVA17_HOME for its later steps")
    # Tests/benchmarks also load Conan-provided shared libraries and UDF examples.
    activation = ROOT / "cpp/build/Release/generators/conanrun.sh"
    if "SPARK_TESTING" in config.get("env", {}):
        env["SPARK_TESTING"] = str(config["env"]["SPARK_TESTING"]).lower()
    else:
        env.pop("SPARK_TESTING", None)
    failures = []
    for number, step in enumerate(steps):
        script = reports / f"step-{number}.sh"
        prefix = "set -euo pipefail\n"
        if not dry_run:
            if not activation.is_file():
                raise ValueError("Missing Conan runtime environment; build native first")
            # The activation is generated by Conan in this checkout, not user input.
            prefix += 'source "' + str(activation) + '"\n'
            prefix += 'export LD_LIBRARY_PATH="' + str(runtime.parent) + ':' + str(runtime) + ':${LD_LIBRARY_PATH:-}"\n'
        script.write_text(prefix + step["script"] + "\n")
        subprocess.run(["bash", "-n", str(script)], check=True)
        if dry_run:
            continue
        # Every reference step starts in the workspace, as it does in Actions.
        start = __import__("time").time()
        command = ["bash", str(script)]
        with (reports / f"step-{number}.log").open("w") as output:
            result = subprocess.run(command, cwd=ROOT, env=env,
                                    stdout=output, stderr=subprocess.STDOUT)
        # Actions applies these files between steps. All reference steps run
        # inside one Python invocation here, so apply their outputs ourselves.
        environment_lines = environment_file.read_text().splitlines()
        for line in environment_lines[environment_offset:]:
            key, separator, value = line.partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z_]\w*", key):
                raise ValueError(f"Unsupported reference environment output: {line}")
            env[key] = value
        environment_offset = len(environment_lines)
        path_lines = path_file.read_text().splitlines()
        additions = path_lines[path_offset:]
        if additions:
            env["PATH"] = os.pathsep.join(reversed(additions)) + os.pathsep + env["PATH"]
        path_offset = len(path_lines)
        error = None
        if result.returncode:
            error = f"exit {result.returncode}"
        else:
            try:
                verify_execution(script.read_text(), (reports / f"step-{number}.log").read_text())
                if re.search(r"\$MVN_CMD\s+(?:clean\s+)?test(?:\s|$)", step["script"]):
                    verify_reports(fresh_reports(ROOT, case, start, spark_only=True))
            except ValueError as exc:
                error = str(exc)
        if error:
            failures.append({"step": step["name"], "error": error})
        # A later reference step can run Maven clean with another Spark profile.
        # Keep the actual reports from this step before that removes them.
        for report in fresh_reports(ROOT, case, start, spark_only=False):
            if reports in report.parents:
                continue
            target = reports / f"step-{number}-xml" / report.relative_to(ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(report, target)
    (reports / "status.json").write_text(json.dumps(
        {"dry_run": dry_run, "executed": not dry_run, "failures": failures}, indent=2) + "\n")
    return bool(failures)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("detect", "plan", "run"))
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--case")
    parser.add_argument("--reports", type=Path, default=ROOT / "bolt-ci-reports")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--flags", help="JSON outputs from the reference change detector")
    args = parser.parse_args()
    if args.command == "detect":
        script = reference()["jobs"]["detect-changes"]["steps"][1]["run"]
        for expression, variable in (("github.event_name", "GITHUB_EVENT_NAME"),
                                     ("github.event.pull_request.base.sha", "BOLT_CI_BASE_SHA"),
                                     ("github.sha", "GITHUB_SHA")):
            script = script.replace("${{ " + expression + " }}", "$" + variable)
        script = script.replace("backends-velox/", "backends-bolt/")
        script = script.replace(
            r"\.github/workflows/",
            r"\.github/((workflows|scripts)/|bolt-ut-blacklist\.json)")
        script = script.replace("ep/build-velox/", r"ep/build-velox/|\.github/bolt-ci-patches/")
        script = script.replace("gluten-ut/(common/|test/|pom", "gluten-ut/(common/|test/|ci/|pom")
        if "${{" in script:
            raise ValueError("Unresolved reference change detector expression")
        return subprocess.run(["bash", "-euo", "pipefail", "-c", script], cwd=ROOT).returncode
    cases = plan(args.full, json.loads(args.flags) if args.flags else None)
    # Validate reference command classification before publishing a matrix.
    for case in cases:
        test_steps(case)
    args.reports.mkdir(parents=True, exist_ok=True)
    (args.reports / "scope.json").write_text(json.dumps({
        "phase": "ut-first",
        "reference_sha256": hashlib.sha256(REFERENCE.read_bytes()).hexdigest(),
        "retained_cases": cases,
        "deferred_jobs": [
            {"job": job, "status": "deferred",
             "reason": "Outside the UT-first phase: " + reason}
            for job, reason in DEFERRED_JOBS.items() if job in reference()["jobs"]
        ],
        "deferred_suite_selection": {
            "status": "deferred",
            "reason": "TPC query and plan stability suites are outside the UT-first phase; "
                      "ordinary UT and CPP benchmark generation remain enabled",
            "environment": {"GLUTEN_BOLT_CI": "true"},
            "selector": "gluten-substrait/src/test/scala/org/apache/gluten/test/"
                        "BoltPhaseOneTestSelection.scala",
        },
    }, indent=2) + "\n")
    if args.command == "plan":
        print(json.dumps({"include": cases}, separators=(",", ":")))
        return 0
    selected = [case for case in cases if case["id"] == args.case]
    if len(selected) != 1:
        parser.error("--case must name an entry from plan")
    (args.reports / "reference-sha256.txt").write_text(
        hashlib.sha256(REFERENCE.read_bytes()).hexdigest() + "\n")
    return int(run_case(selected[0], args.reports, args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
