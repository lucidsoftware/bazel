#!/usr/bin/env bash
set -euo pipefail

ARTIFACT="${1:-${ARTIFACT:-bazel-9.2.999-linux-arm64}}"
EXPECTED_VERSION="${EXPECTED_VERSION:-9.2.999}"
MAX_GLIBC_EXCLUSIVE="${MAX_GLIBC_EXCLUSIVE:-2.38}"
MAX_GLIBCXX_EXCLUSIVE="${MAX_GLIBCXX_EXCLUSIVE:-3.4.32}"

if [[ ! -f "${ARTIFACT}" ]]; then
  echo "artifact not found: ${ARTIFACT}" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

ARTIFACT_DIR="$(cd "$(dirname "${ARTIFACT}")" && pwd)"
ARTIFACT_NAME="$(basename "${ARTIFACT}")"

docker run --rm --platform linux/arm64 \
  -e EXPECTED_VERSION="${EXPECTED_VERSION}" \
  -e MAX_GLIBC_EXCLUSIVE="${MAX_GLIBC_EXCLUSIVE}" \
  -e MAX_GLIBCXX_EXCLUSIVE="${MAX_GLIBCXX_EXCLUSIVE}" \
  -v "${ARTIFACT_DIR}/${ARTIFACT_NAME}:/artifact/source:ro" \
  ubuntu:22.04 bash -lc '
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update >/dev/null
apt-get install -y --no-install-recommends binutils file libc6 libstdc++6 unzip >/dev/null

install -m 0755 /artifact/source /tmp/bazel-validation-binary
binary="/tmp/bazel-validation-binary"

if [[ "$(uname -m)" != "aarch64" ]]; then
  echo "ERROR: validation container is not AArch64" >&2
  exit 1
fi

file_output="$(file "${binary}")"
if [[ "${file_output}" != *"ARM aarch64"* && "${file_output}" != *"AArch64"* ]]; then
  echo "ERROR: artifact is not an AArch64 executable: ${file_output}" >&2
  exit 1
fi

embedded_label="$(unzip -p "${binary}" build-label.txt)"
if [[ "${embedded_label}" != "${EXPECTED_VERSION}" ]]; then
  echo "ERROR: embedded build label is ${embedded_label}, expected ${EXPECTED_VERSION}" >&2
  exit 1
fi

validation_workspace="$(mktemp -d /tmp/bazel-validation-workspace.XXXXXX)"
printf "%s\n" "module(name = \"bazel_release_validation\")" >"${validation_workspace}/MODULE.bazel"
printf "%s\n" \
  "genrule(" \
  "    name = \"smoke\"," \
  "    outs = [\"smoke.txt\"]," \
  "    cmd = \"echo bazel-${EXPECTED_VERSION}-smoke-ok > \$@\"," \
  ")" >"${validation_workspace}/BUILD.bazel"
cd "${validation_workspace}"

release="$("${binary}" --batch --output_user_root=/tmp/bazel-validation info release)"
if [[ "${release}" != "release ${EXPECTED_VERSION}" ]]; then
  echo "ERROR: artifact reports ${release}, expected release ${EXPECTED_VERSION}" >&2
  exit 1
fi

echo "Artifact:"
echo "${file_output}"

echo
echo "Embedded build label: ${embedded_label}"
echo "Reported release: ${release}"

echo
echo "Dynamic libraries:"
ldd_output="$(ldd "${binary}")"
echo "${ldd_output}"
if grep -q "not found" <<<"${ldd_output}"; then
  echo "ERROR: one or more dynamic libraries were not found" >&2
  exit 1
fi

echo
echo "Version requirements from readelf:"
readelf --dyn-syms --wide "${binary}" \
  | grep -Eo "GLIBC_[0-9.]+|GLIBCXX_[0-9.]+" \
  | sort -Vu \
  | tee /tmp/readelf-symbol-versions.txt

echo
echo "Version strings:"
strings "${binary}" \
  | grep -Eo "GLIBC_[0-9.]+|GLIBCXX_[0-9.]+" \
  | sort -Vu \
  | tee /tmp/strings-symbol-versions.txt

max_glibc="$(sed -n "s/^GLIBC_//p" /tmp/strings-symbol-versions.txt | sort -Vu | tail -n 1)"
max_glibcxx="$(sed -n "s/^GLIBCXX_//p" /tmp/strings-symbol-versions.txt | sort -Vu | tail -n 1)"

echo
echo "Maximum required GLIBC: ${max_glibc:-none found}"
echo "Maximum required GLIBCXX: ${max_glibcxx:-none found}"

version_at_least() {
  [[ "$(printf "%s\n%s\n" "$1" "$2" | sort -V | tail -n 1)" == "$1" ]]
}

if [[ -n "${max_glibc}" ]] && version_at_least "${max_glibc}" "${MAX_GLIBC_EXCLUSIVE}"; then
  echo "ERROR: artifact requires GLIBC_${max_glibc}, must be older than GLIBC_${MAX_GLIBC_EXCLUSIVE}" >&2
  exit 1
fi
if [[ -n "${max_glibcxx}" ]] && version_at_least "${max_glibcxx}" "${MAX_GLIBCXX_EXCLUSIVE}"; then
  echo "ERROR: artifact requires GLIBCXX_${max_glibcxx}, must be older than GLIBCXX_${MAX_GLIBCXX_EXCLUSIVE}" >&2
  exit 1
fi

echo
echo "Smoke test:"
"${binary}" --batch --output_user_root=/tmp/bazel-smoke-output build //:smoke
smoke_output="$(sed -n "1p" bazel-bin/smoke.txt)"
expected_smoke="bazel-${EXPECTED_VERSION}-smoke-ok"
if [[ "${smoke_output}" != "${expected_smoke}" ]]; then
  echo "ERROR: smoke output is ${smoke_output}, expected ${expected_smoke}" >&2
  exit 1
fi
echo "${smoke_output}"

echo
echo "SHA-256:"
sha256sum "${binary}"

echo
echo "OK: AArch64, release metadata, Jammy ABI ceiling, libraries, and smoke build validated."
'
