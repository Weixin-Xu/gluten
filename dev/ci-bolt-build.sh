#!/usr/bin/env bash

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

set -euo pipefail

GLUTEN_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BOLT_ROOT=$(cd "${1:?Usage: ci-bolt-build.sh BOLT_CHECKOUT}" && pwd)
export PATH="${HOME}/miniconda3/bin:${PATH}"
export CC=${CC:-gcc-12}
export CXX=${CXX:-g++-12}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export PATH="${JAVA_HOME}/bin:${PATH}"
BUILD_THREADS=${CI_NUM_THREADS:-$(nproc)}
# The reference x86 tests include MinIO/S3; keep native support enabled.
S3_ENABLED=True

for tool in git make cmake ninja conan javac; do
  command -v "$tool" > /dev/null || { echo "Missing environment prerequisite: $tool" >&2; exit 1; }
done

# Use the standard Bolt setup script before this entry point. The build version
# binds the consumed Conan package to the checked-out candidate, never remote main.
BOLT_SHA=$(git -C "$BOLT_ROOT" rev-parse HEAD)
GLUTEN_SHA=$(git -C "$GLUTEN_ROOT" rev-parse HEAD)
mkdir -p "$GLUTEN_ROOT/bolt-ci-reports/native"
git -C "$BOLT_ROOT" diff --exit-code HEAD --
# Apply any local compatibility fixes in an isolated checkout. Bind the
# package version to both the pinned revision and every compatibility patch.
python3 - "$BOLT_ROOT" "$GLUTEN_ROOT" <<'PY_COMPAT'
import hashlib
import json
import subprocess
import sys
from pathlib import Path

original, gluten = map(Path, sys.argv[1:])
reports = gluten / "bolt-ci-reports/native"

def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

revision = git(original, "rev-parse", "HEAD")
patches = sorted((gluten / ".github/bolt-ci-patches").glob("*.patch"))
patch_hashes = {patch.name: digest(patch) for patch in patches}
patch_set = hashlib.sha256(json.dumps(patch_hashes, sort_keys=True).encode()).hexdigest()
source = gluten / ".cache/bolt-ci-source" / f"{revision}-{patch_set[:16]}"
record = source.with_suffix(".json")
source.parent.mkdir(parents=True, exist_ok=True)
if not source.exists():
    subprocess.run(["git", "-C", str(original), "worktree", "add", "--detach",
                    str(source), revision], check=True)
    applied = []
    for patch in patches:
        command = ["git", "-C", str(source), "apply"]
        check = subprocess.run(command + ["--check", str(patch)], capture_output=True)
        if check.returncode == 0:
            subprocess.run(command + [str(patch)], check=True)
            applied.append(patch.name)
        else:
            # Accept an already integrated fix only when its reverse matches.
            subprocess.run(command + ["--reverse", "--check", str(patch)], check=True)
    changed = git(source, "diff", "--name-only", "HEAD", "--").splitlines()
    proof = {"revision": revision, "patch_set_sha256": patch_set,
             "patches": patch_hashes, "applied": applied,
             "changed_files": {name: digest(source / name) for name in changed}}
    record.write_text(json.dumps(proof, indent=2) + "\n")
else:
    proof = json.loads(record.read_text())
    changed = git(source, "diff", "--name-only", "HEAD", "--").splitlines()
    actual = {name: digest(source / name) for name in changed}
    if (git(source, "rev-parse", "HEAD") != revision or
            proof["revision"] != revision or proof["patches"] != patch_hashes or
            proof["patch_set_sha256"] != patch_set or actual != proof["changed_files"]):
        raise RuntimeError("Existing isolated Bolt source does not match its pinned patches")
if git(source, "ls-files", "--others", "--exclude-standard"):
    raise RuntimeError("Unexpected untracked files in isolated Bolt source")
(reports / "bolt-compat-source.json").write_text(json.dumps(proof, indent=2) + "\n")
(reports / "effective-bolt-source.txt").write_text(str(source) + "\n")
(reports / "bolt-build-version.txt").write_text(f"ci-{revision}-compat-{patch_set[:16]}\n")
PY_COMPAT
BOLT_ROOT=$(cat "$GLUTEN_ROOT/bolt-ci-reports/native/effective-bolt-source.txt")
export BOLT_BUILD_VERSION
BOLT_BUILD_VERSION=$(cat "$GLUTEN_ROOT/bolt-ci-reports/native/bolt-build-version.txt")
git -C "${CONAN_HOME:-${HOME}/.conan2}/conan-center-index" rev-parse HEAD \
  > "$GLUTEN_ROOT/bolt-ci-reports/native/conan-recipes-sha.txt"
printf '%s\n' "$BOLT_SHA" > "$GLUTEN_ROOT/bolt-ci-reports/native/bolt-sha.txt"

BOLT_OPTIONS=(-o 'bolt/*:spark_compatible=True' -o 'bolt/*:enable_testutil=True'
  -o 'bolt/*:shared=False' -o 'bolt/*:enable_hdfs=True' -o "bolt/*:enable_s3=$S3_ENABLED"
  -o 'bolt/*:enable_asan=False')
HOST_PROFILE=(-pr:h default)
# Older Bolt revisions provided an overlay; current setup configures default directly.
if [[ -f "$BOLT_ROOT/scripts/conan/bolt.profile" ]]; then
  HOST_PROFILE+=(-pr:h "$BOLT_ROOT/scripts/conan/bolt.profile")
fi
PROFILE=("${HOST_PROFILE[@]}" -s build_type=Release)
printf -v HOST_PROFILE_ARGS '%q ' "${HOST_PROFILE[@]}"

cd "$BOLT_ROOT"
make release_spark_with_test BOLT_CONAN_CONFIGURE_ONLY=1 \
  "BUILD_VERSION=$BOLT_BUILD_VERSION" "CONAN_OVERRIDE=${BOLT_OPTIONS[*]}" \
  "CONAN_HOST_PROFILE_ARGS=$HOST_PROFILE_ARGS"
# CMake itself is a Conan tool requirement. Use its generated build environment,
# rather than Ubuntu's older system CMake for the exported presets.
source "$BOLT_ROOT/_build/Release/generators/conanbuild.sh"
cmake --build --preset conan-release --target bolt_engine bolt_testutils_static \
  --parallel "$BUILD_THREADS"
conan export-pkg . --name=bolt "--version=$BOLT_BUILD_VERSION" \
  "${PROFILE[@]}" "${BOLT_OPTIONS[@]}" -c 'bolt/*:tools.build:skip_test=False' \
  --format=json > "$GLUTEN_ROOT/bolt-ci-reports/native/bolt-export.json"

cd "$GLUTEN_ROOT/cpp"
conan install . --name=gluten "--version=ci-${GLUTEN_SHA}" \
  "${PROFILE[@]}" "${BOLT_OPTIONS[@]}" \
  -o 'gluten/*:shared=True' -o 'gluten/*:build_tests=True' \
  -o 'gluten/*:build_examples=True' -o 'gluten/*:build_benchmarks=True' \
  -o 'gluten/*:enable_hdfs=True' -o "gluten/*:enable_s3=$S3_ENABLED" -o 'gluten/*:enable_asan=False' \
  -c 'arrow/*:tools.build:download_source=True' \
  --build=missing --build='~bolt/*' --lockfile= --format=json \
  > "$GLUTEN_ROOT/bolt-ci-reports/native/gluten-install.json"
printf '%s\n' "$S3_ENABLED" > "$GLUTEN_ROOT/cpp/build/bolt-ci-s3-enabled.txt"
python3 - "$GLUTEN_ROOT/bolt-ci-reports/native" <<'PY'
import json
import sys
from pathlib import Path

reports = Path(sys.argv[1])
def identity(name):
    nodes = json.loads((reports / name).read_text())["graph"]["nodes"].values()
    bolt = [node for node in nodes if node.get("name") == "bolt" and node.get("context") == "host"]
    if len(bolt) != 1:
        raise RuntimeError("Expected exactly one Bolt host package")
    return {key: bolt[0].get(key) for key in ("ref", "package_id", "prev")}
exported = identity("bolt-export.json")
consumed = identity("gluten-install.json")
if any(not value for value in exported.values()) or exported != consumed:
    raise RuntimeError(f"Gluten did not consume the locally exported Bolt binary: {exported} / {consumed}")
(reports / "bolt-package.json").write_text(json.dumps(consumed, indent=2) + "\n")
PY
source "$GLUTEN_ROOT/cpp/build/Release/generators/conanbuild.sh"
cmake --preset conan-release
cmake --build --preset conan-release --parallel "$BUILD_THREADS"

# Preserve the shared-library closure when the native artifact moves to a test
# runner that has no Conan cache. System libraries come from the same Ubuntu OS.
python3 - "$GLUTEN_ROOT/cpp/build" <<'PY'
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
destination = root / "releases/third-party-runtime"
destination.mkdir(parents=True, exist_ok=True)
for binary in list(root.rglob("*")):
    if not binary.is_file() or destination in binary.parents:
        continue
    with binary.open("rb") as stream:
        if stream.read(4) != b"\x7fELF":
            continue
    environment = dict(os.environ)
    environment["LD_LIBRARY_PATH"] = os.pathsep.join((
        str(root / "releases"), str(destination), environment.get("LD_LIBRARY_PATH", "")))
    result = subprocess.run(["ldd", str(binary)], capture_output=True, text=True, env=environment)
    if "=> not found" in result.stdout:
        raise RuntimeError(f"Incomplete native dependency closure: {binary}\n{result.stdout}")
    for path in re.findall(r"=> (/\S+) ", result.stdout):
        source = Path(path)
        if ".conan2" not in source.parts and "CONAN_HOME" not in os.environ:
            continue
        cache = Path(os.environ.get("CONAN_HOME", str(Path.home() / ".conan2")))
        if cache not in source.parents:
            continue
        target = destination / source.name
        if target.exists() and target.read_bytes() != source.read_bytes():
            raise RuntimeError(f"Conflicting runtime libraries: {source.name}")
        shutil.copy2(source, target)
PY

# Discover with the same Conan CMake that generated the PRE_TEST include files.
# Preserve the resulting test lists in the artifact so test runners do not need
# the build runner's Conan cache or its CMake GoogleTest module.
LD_LIBRARY_PATH="$GLUTEN_ROOT/cpp/build/releases:${LD_LIBRARY_PATH:-}" \
  "$(dirname "$(command -v cmake)")/ctest" --test-dir "$GLUTEN_ROOT/cpp/build/Release" -N

cd "$GLUTEN_ROOT"
# The Arrow script invokes mvn. Route it through Gluten's pinned Maven wrapper.
MAVEN_SHIM=$(mktemp -d)
trap 'rm -rf "$MAVEN_SHIM"' EXIT
export BOLT_MAVEN_SHIM="$MAVEN_SHIM"
# The wrapper probes a system mvn; hide this adapter to avoid re-entering it.
printf '#!/usr/bin/env bash\nexport PATH="${PATH#"$BOLT_MAVEN_SHIM:"}"\nexec "%s/build/mvn" "$@"\n' \
  "$GLUTEN_ROOT" > "$MAVEN_SHIM/mvn"
chmod +x "$MAVEN_SHIM/mvn"
PATH="$MAVEN_SHIM:$PATH" bash dev/build_bolt_arrow.sh
test -f cpp/build/releases/libbolt_backend.so
test -f cpp/build/package/bolt/linux/amd64/libbolt_backend.so
