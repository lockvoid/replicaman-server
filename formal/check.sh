#!/usr/bin/env bash
# Exhaustive finite TLC runs; logs, states and downloaded tools stay outside source.
set -euo pipefail
formal_dir=$(cd "$(dirname "$0")" && pwd)
tlc_version=1.7.4
tlc_sha256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
cache_dir=${XDG_CACHE_HOME:-"$HOME/.cache"}/replicaman-tlc
tlc_jar=${TLA_JAR:-"$cache_dir/tla2tools-$tlc_version.jar"}
java_bin=${JAVA_BIN:-java}

if [[ ! -f "$tlc_jar" ]]; then
    mkdir -p "$(dirname "$tlc_jar")"
    partial=$(mktemp "${tlc_jar}.XXXXXX")
    trap 'rm -f "$partial"' EXIT
    curl --fail --location --silent --show-error \
        "https://github.com/tlaplus/tlaplus/releases/download/v$tlc_version/tla2tools.jar" \
        --output "$partial"
    actual=$(shasum -a 256 "$partial")
    [[ ${actual%% *} == "$tlc_sha256" ]] || { printf 'TLC checksum mismatch\n' >&2; exit 1; }
    mv "$partial" "$tlc_jar"
    trap - EXIT
fi
actual=$(shasum -a 256 "$tlc_jar")
[[ ${actual%% *} == "$tlc_sha256" ]] || { printf 'TLC checksum mismatch\n' >&2; exit 1; }
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/replicaman-tlc.XXXXXX")
printf 'TLC %s; logs and counterexamples: %s\n' "$tlc_version" "$run_dir"
cd "$formal_dir"

check() {
    local module=$1 config=$2 expected_status=$3 expected_text=$4 status log output
    log="$run_dir/$config.log"
    if "$java_bin" -XX:+UseParallelGC -Xmx1g -cp "$tlc_jar" tlc2.TLC \
        -workers 1 -fp 0 -seed 1 -metadir "$run_dir/$config-states" \
        -config "$config.cfg" "$module.tla" >"$log" 2>&1; then
        status=0
    else
        status=$?
    fi
    output=$(<"$log")
    if [[ $status != "$expected_status" || $output != *"$expected_text"* ]]; then
        printf 'FAIL %s: expected exit %s and %s, got exit %s\n' \
            "$config" "$expected_status" "$expected_text" "$status" >&2
        cat "$log" >&2
        exit 1
    fi
    if [[ $expected_status == 0 ]]; then
        printf 'PASS %s: ' "$config"
        awk '/states generated, .* distinct states found, 0 states left on queue\./ {line=$0} END {print line}' "$log"
    else
        printf 'PASS %s: expected counterexample (%s)\n' "$config" "$expected_text"
    fi
}

check Delivery Delivery 0 'Model checking completed. No error has been found.'
check Read Read 0 'Model checking completed. No error has been found.'
check Recovery Recovery 0 'Model checking completed. No error has been found.'
check Joint Joint 0 'Model checking completed. No error has been found.'
check Delivery DeliveryBrokenDedup 12 'Invariant AtMostOnce is violated.'
check Read ReadBrokenCursor 12 'Invariant ConfirmedPrefix is violated.'
check Read ReadBrokenBarrier 12 'Invariant LocalConservation is violated.'
check Recovery RecoveryLegacy 13 'Temporal properties were violated.'
check Joint JointBrokenUnseen 12 'Invariant KnownReceiptNeverRegresses is violated.'
check Joint JointBrokenSession 12 'Invariant OldResponseNoMutation is violated.'
printf 'All four finite models passed; all six deliberate defects were detected.\n'
