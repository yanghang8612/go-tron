#!/usr/bin/env bash
set -euo pipefail

archive="${1:-third_party/librustzcash/target/release/librustzcash.a}"
if [[ ! -f "$archive" ]]; then
  echo "error: static archive not found: $archive" >&2
  exit 1
fi

rust_sysroot=""
find_rust_objcopy() {
  command -v rustc >/dev/null 2>&1 || return 1
  rust_sysroot="$(rustc --print sysroot)"
  local host
  host="$(rustc -vV | awk '/^host:/{print $2}')"
  local bin="$rust_sysroot/lib/rustlib/$host/bin/rust-objcopy"
  [[ -x "$bin" ]] || return 1
  printf '%s\n' "$bin"
}

tmp="$(dirname "$archive")/.$(basename "$archive").strip.$$"
trap 'rm -f "$tmp"' EXIT

os="$(uname -s)"
objcopy_bin=""
is_rust_objcopy=0

if [[ "$os" == "Darwin" ]]; then
  if objcopy_bin="$(command -v llvm-objcopy 2>/dev/null)"; then
    :
  elif objcopy_bin="$(find_rust_objcopy)"; then
    is_rust_objcopy=1
  else
    echo "error: neither llvm-objcopy nor rust-objcopy found; install llvm or Rust toolchain" >&2
    exit 1
  fi

  if ((is_rust_objcopy)); then
    DYLD_LIBRARY_PATH="$rust_sysroot/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" \
      "$objcopy_bin" --wildcard \
      --remove-section='__LLVM,*' \
      --remove-section='__GNU_LTO,*' \
      --remove-section='*,.llvmbc' \
      --remove-section='*,.llvmcmd' \
      --remove-section='*,.llvm.lto*' \
      --remove-section='*,.gnu.lto_*' \
      --remove-section='*,.gnu.debuglto_*' \
      "$archive" "$tmp"
  else
    "$objcopy_bin" --wildcard \
      --remove-section='__LLVM,*' \
      --remove-section='__GNU_LTO,*' \
      --remove-section='*,.llvmbc' \
      --remove-section='*,.llvmcmd' \
      --remove-section='*,.llvm.lto*' \
      --remove-section='*,.gnu.lto_*' \
      --remove-section='*,.gnu.debuglto_*' \
      "$archive" "$tmp"
  fi
else
  if [[ -n "${CC:-}" ]]; then
    cc_dir="$(dirname "$CC")"
    if [[ -x "$cc_dir/objcopy" ]]; then
      objcopy_bin="$cc_dir/objcopy"
    fi
  fi
  if [[ -z "$objcopy_bin" ]] && objcopy_bin="$(command -v llvm-objcopy 2>/dev/null)"; then
    :
  elif [[ -z "$objcopy_bin" ]] && objcopy_bin="$(command -v objcopy 2>/dev/null)"; then
    :
  elif [[ -z "$objcopy_bin" ]] && objcopy_bin="$(find_rust_objcopy)"; then
    is_rust_objcopy=1
  fi

  if [[ -z "$objcopy_bin" ]]; then
    echo "error: no suitable objcopy (CC-adjacent objcopy, llvm-objcopy, objcopy, or rust-objcopy) found" >&2
    exit 1
  fi

  if ((is_rust_objcopy)); then
    version_out="$(LD_LIBRARY_PATH="$rust_sysroot/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$objcopy_bin" --version 2>&1 || true)"
  else
    version_out="$("$objcopy_bin" --version 2>&1 || true)"
  fi

  if grep -qi 'llvm' <<<"$version_out"; then
    if ((is_rust_objcopy)); then
      LD_LIBRARY_PATH="$rust_sysroot/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$objcopy_bin" --wildcard \
        --remove-section=.llvmbc \
        --remove-section=.llvmcmd \
        --remove-section='.llvm.lto*' \
        --remove-section='.gnu.lto_*' \
        --remove-section='.gnu.debuglto_*' \
        "$archive" "$tmp"
    else
      "$objcopy_bin" --wildcard \
        --remove-section=.llvmbc \
        --remove-section=.llvmcmd \
        --remove-section='.llvm.lto*' \
        --remove-section='.gnu.lto_*' \
        --remove-section='.gnu.debuglto_*' \
        "$archive" "$tmp"
    fi
  else
    "$objcopy_bin" \
      --remove-section=.llvmbc \
      --remove-section=.llvmcmd \
      --remove-section='.llvm.lto*' \
      --remove-section='.gnu.lto_*' \
      --remove-section='.gnu.debuglto_*' \
      "$archive" "$tmp"
  fi
fi

ranlib_bin=""
if [[ -n "${CC:-}" ]]; then
  cc_dir="$(dirname "$CC")"
  if [[ -x "$cc_dir/ranlib" ]]; then
    ranlib_bin="$cc_dir/ranlib"
  fi
fi
if [[ -z "$ranlib_bin" ]] && ranlib_bin="$(command -v ranlib 2>/dev/null)"; then
  :
fi
if [[ -n "$ranlib_bin" ]]; then
  "$ranlib_bin" "$tmp"
fi

if cmp -s "$tmp" "$archive"; then
  exit 0
fi

mv -f "$tmp" "$archive"
