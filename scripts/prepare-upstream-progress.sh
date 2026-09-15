#!/bin/sh
set -eu

tdl_version="v0.20.4"
patch_file="patches/tdl-progress-0.20.4.patch"
runtime_patch="patches/tdl-runtime-options-0.20.4.patch"
cancel_patch="patches/tdl-cancel-0.20.4.patch"
target_dir=".upstream/tdl"

if [ ! -f "$patch_file" ]; then
  echo "missing upstream progress patch: $patch_file" >&2
  exit 1
fi
if [ ! -f "$runtime_patch" ]; then
  echo "missing upstream runtime options patch: $runtime_patch" >&2
  exit 1
fi
if [ ! -f "$cancel_patch" ]; then
  echo "missing upstream cancellation patch: $cancel_patch" >&2
  exit 1
fi

# go.work points to the patched copy, which does not exist until this script
# completes, so obtain the official module with workspace mode disabled first.
GOWORK=off go mod download "github.com/iyear/tdl@$tdl_version"
module_dir="$(GOWORK=off go env GOMODCACHE)/github.com/iyear/tdl@$tdl_version"
if [ ! -f "$module_dir/app/dl/dl.go" ] || [ ! -f "$module_dir/app/dl/progress.go" ]; then
  echo "unsupported upstream tdl layout for $tdl_version" >&2
  exit 1
fi

expected_dl="d97fdc3dbf3258e3c8fa6224c043630b978f6b2355b3ce3679c8e10de76fca21"
expected_progress="bf3c26e927480efc88de13a3bb81960b932195538b0c26427d8ed6b10b620d5b"
actual_dl="$(sha256sum "$module_dir/app/dl/dl.go" | awk '{print $1}')"
actual_progress="$(sha256sum "$module_dir/app/dl/progress.go" | awk '{print $1}')"
if [ "$actual_dl" != "$expected_dl" ] || [ "$actual_progress" != "$expected_progress" ]; then
  echo "upstream tdl source checksum differs from the supported v0.20.4 release; refusing to apply progress patch" >&2
  exit 1
fi

if [ -e "$target_dir" ]; then
  rm -rf "$target_dir" || {
    echo "cannot remove generated directory $target_dir; remove it and retry" >&2
    exit 1
  }
fi
mkdir -p "$(dirname "$target_dir")"
# Go's module cache deliberately marks source files read-only. Do not preserve
# those modes here: this is a disposable local copy which needs to be patched
# by the regular Dev Container user as well as by the image build user.
cp -R "$module_dir" "$target_dir"
chmod -R u+w "$target_dir"

if ! (cd "$target_dir" && patch --batch --forward -p1 < "../../$patch_file"); then
  echo "tdl progress patch did not apply; refusing to build an unpatched image" >&2
  rm -rf "$target_dir"
  exit 1
fi
if ! (cd "$target_dir" && patch --batch --forward -p1 < "../../$runtime_patch"); then
  echo "tdl runtime options patch did not apply; refusing to build an unsafe concurrent image" >&2
  rm -rf "$target_dir"
  exit 1
fi
if ! (cd "$target_dir" && patch --batch --forward -p1 < "../../$cancel_patch"); then
  echo "tdl cancellation patch did not apply; refusing to build an unsafe cancellation image" >&2
  rm -rf "$target_dir"
  exit 1
fi

echo "Prepared patched upstream tdl $tdl_version"
