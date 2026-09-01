#!/usr/bin/env bash
set -euo pipefail

ARTIFACT="${1:-${ARTIFACT:-bazel-9.2.999-linux-arm64}}"
EXPECTED_VERSION="${EXPECTED_VERSION:-9.2.999}"
FORBIDDEN_PATTERN="${FORBIDDEN_PATTERN:-GLIBC_2\.38|GLIBCXX_3\.4\.32}"

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
  -e ARTIFACT_NAME="${ARTIFACT_NAME}" \
  -e EXPECTED_VERSION="${EXPECTED_VERSION}" \
  -e FORBIDDEN_PATTERN="${FORBIDDEN_PATTERN}" \
  -v "${ARTIFACT_DIR}:/workspace:ro" \
  -w /workspace \
  ubuntu:22.04 bash -lc '
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update >/dev/null
apt-get install -y --no-install-recommends binutils file libc6 libstdc++6 unzip >/dev/null

embedded_label="$(unzip -p "${ARTIFACT_NAME}" build-label.txt)"
if [[ "${embedded_label}" != "${EXPECTED_VERSION}" ]]; then
  echo "ERROR: embedded build label is ${embedded_label}, expected ${EXPECTED_VERSION}" >&2
  exit 1
fi

release="$("./${ARTIFACT_NAME}" --batch --output_user_root=/tmp/bazel-validation info release)"
if [[ "${release}" != "release ${EXPECTED_VERSION}" ]]; then
  echo "ERROR: artifact reports ${release}, expected release ${EXPECTED_VERSION}" >&2
  exit 1
fi

echo "Artifact:"
file "${ARTIFACT_NAME}"

echo
echo "Embedded build label: ${embedded_label}"
echo "Reported release: ${release}"

echo
echo "Dynamic libraries:"
ldd "${ARTIFACT_NAME}"

echo
echo "Version requirements from readelf:"
readelf --version-info "${ARTIFACT_NAME}" | grep -E "GLIBC|GLIBCXX" | sort -Vu

echo
echo "Version strings:"
strings "${ARTIFACT_NAME}" | grep -Eo "GLIBC_[0-9.]+|GLIBCXX_[0-9.]+" | sort -Vu

max_glibc="$(strings "${ARTIFACT_NAME}" | grep -Eo "GLIBC_[0-9.]+" | sed "s/^GLIBC_//" | sort -Vu | tail -n 1 || true)"
max_glibcxx="$(strings "${ARTIFACT_NAME}" | grep -Eo "GLIBCXX_[0-9.]+" | sed "s/^GLIBCXX_//" | sort -Vu | tail -n 1 || true)"

echo
echo "Maximum required GLIBC: ${max_glibc:-none found}"
echo "Maximum required GLIBCXX: ${max_glibcxx:-none found}"

if strings "${ARTIFACT_NAME}" | grep -Eq "${FORBIDDEN_PATTERN}"; then
  echo "ERROR: artifact still requires a forbidden runtime symbol version" >&2
  exit 1
fi

echo
echo "OK: no forbidden GLIBC_2.38 or GLIBCXX_3.4.32 requirement found."
'
