#!/usr/bin/env bash
# run_mpc_direct.sh — Benchmark MP-SPDZ protocols directly on localhost (no Veilid).
#
# For each (protocol, num_parties) configuration and each iteration:
#   1. Ensures the MPC schedule is compiled.
#   2. Writes a localhost HOSTS file (N lines of 127.0.0.1).
#   3. Spawns N party processes in parallel, each receiving a random bid via stdin.
#   4. Waits for all to finish, captures party 0's stderr.
#   5. Parses MP-SPDZ timing/traffic stats from party 0's stderr.
#   6. Appends a CSV row to $BENCH_OUT.
#
# Usage:
#   MP_SPDZ_DIR=/path/to/MP-SPDZ bash scripts/bench/run_mpc_direct.sh
#
# Environment variables:
#   BENCH_PROTOCOLS  — space-separated protocol names  (default: "mascot shamir rep-ring")
#   BENCH_PARTIES    — space-separated party counts    (default: "3 4 5 6 8 10")
#   BENCH_ITERS      — iterations per configuration   (default: 5)
#   MP_SPDZ_DIR      — path to MP-SPDZ checkout       (default: ../../MP-SPDZ)
#   BENCH_OUT        — output CSV file                 (default: bench-results/direct_mpc.csv)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BENCH_PROTOCOLS="${BENCH_PROTOCOLS:-mascot shamir rep-ring}"
BENCH_PARTIES="${BENCH_PARTIES:-3 4 5 6 8 10 15 20}"
# Per-protocol party count caps. MASCOT's O(N²) pairwise OT uses ~2GB/party,
# so 10 parties ≈ 20GB RAM. Shamir's O(N) communication scales to 20 easily.
BENCH_MAX_PARTIES_MASCOT="${BENCH_MAX_PARTIES_MASCOT:-10}"
BENCH_MAX_PARTIES_SHAMIR="${BENCH_MAX_PARTIES_SHAMIR:-20}"
BENCH_ITERS="${BENCH_ITERS:-5}"
MP_SPDZ_DIR="${MP_SPDZ_DIR:-../../MP-SPDZ}"
BENCH_OUT="${BENCH_OUT:-bench-results/direct_mpc.csv}"

# Base port for the first iteration; each subsequent iteration adds 100.
# Ports used per iteration: base_port .. base_port + N - 1.
BASE_PORT_START=11000

# ---------------------------------------------------------------------------
# Resolve MP_SPDZ_DIR to an absolute path
# ---------------------------------------------------------------------------
if command -v realpath &>/dev/null; then
    MP_SPDZ_DIR="$(realpath -- "$MP_SPDZ_DIR")"
else
    MP_SPDZ_DIR="$(cd -- "$MP_SPDZ_DIR" && pwd)"
fi

if [[ ! -d "$MP_SPDZ_DIR" ]]; then
    echo "ERROR: MP_SPDZ_DIR='$MP_SPDZ_DIR' does not exist." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Temp file tracking — all temp files are registered here.
# The EXIT trap removes them even on error.
# ---------------------------------------------------------------------------
declare -a _TEMP_FILES=()

_cleanup() {
    for f in "${_TEMP_FILES[@]+"${_TEMP_FILES[@]}"}"; do
        rm -f -- "$f"
    done
}
trap _cleanup EXIT

# make_temp <suffix>
# Creates a temp file, registers it for cleanup, and prints its path.
make_temp() {
    local suffix="${1:-}"
    local tmp
    tmp="$(mktemp "/tmp/bench_mpc_XXXXXX${suffix}")"
    _TEMP_FILES+=("$tmp")
    printf '%s' "$tmp"
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log_progress() { echo "[$(date '+%H:%M:%S')] $*" >&2; }
log_warn()     { echo "WARNING: $*" >&2; }

# protocol_binary <name> — maps a friendly protocol name to the MP-SPDZ binary.
protocol_binary() {
    case "$1" in
        rep-ring) echo "replicated-ring-party.x" ;;
        *)        echo "${1}-party.x" ;;
    esac
}

# is_ring_protocol <name> — returns 0 (true) for replicated-ring.
is_ring_protocol() { [[ "$1" == "rep-ring" ]]; }

# random_bid — a random integer in [100, 10000].
random_bid() { echo $(( RANDOM % 9901 + 100 )); }

# wall_clock — current time as a decimal seconds string.
# Uses EPOCHREALTIME (bash 5+) if available; falls back to date +%s.%N.
wall_clock() {
    if [[ -n "${EPOCHREALTIME+x}" ]]; then
        printf '%s' "$EPOCHREALTIME"
    else
        date +%s.%N
    fi
}

# parse_mpc_stats <stderr_file>
# Scans party 0's stderr for the standard MP-SPDZ statistics lines and
# prints four space-separated values: mpc_time_secs data_sent_mb rounds global_data_mb
# Any field not found is printed as an empty string.
parse_mpc_stats() {
    local file="$1"
    local mpc_time="" data_mb="" rounds="" global_mb=""

    while IFS= read -r line; do
        # "Time = 8.312 seconds"
        if [[ "$line" =~ ^Time\ =\ ([0-9]+\.?[0-9]*)\ seconds ]]; then
            mpc_time="${BASH_REMATCH[1]}"
        fi
        # "Data sent = 1.23 MB in ~456 rounds (party 0 only...)"
        if [[ "$line" =~ Data\ sent\ =\ ([0-9]+\.?[0-9]*)\ MB\ in\ ~([0-9]+)\ rounds ]]; then
            data_mb="${BASH_REMATCH[1]}"
            rounds="${BASH_REMATCH[2]}"
        fi
        # "Global data sent = 3.45 MB (all parties)"
        if [[ "$line" =~ Global\ data\ sent\ =\ ([0-9]+\.?[0-9]*)\ MB ]]; then
            global_mb="${BASH_REMATCH[1]}"
        fi
    done <"$file"

    printf '%s %s %s %s' "$mpc_time" "$data_mb" "$rounds" "$global_mb"
}

# ---------------------------------------------------------------------------
# Ensure output directory and write CSV header (only when file is new/empty)
# ---------------------------------------------------------------------------
mkdir -p "$(dirname "$BENCH_OUT")"

CSV_HEADER="timestamp,protocol,num_parties,iteration,wall_clock_secs,mpc_time_secs,data_sent_mb,rounds,global_data_mb"
if [[ ! -f "$BENCH_OUT" || ! -s "$BENCH_OUT" ]]; then
    printf '%s\n' "$CSV_HEADER" >"$BENCH_OUT"
fi

# ---------------------------------------------------------------------------
# Ensure SSL certificates are fresh (needed by Shamir and Rep-Ring).
# MASCOT uses OT-based key exchange so it doesn't need certs.
# ---------------------------------------------------------------------------
max_parties=3
for n in $BENCH_PARTIES; do
    if (( n > max_parties )); then max_parties=$n; fi
done
if [[ ! -f "${MP_SPDZ_DIR}/Player-Data/P0.pem" ]] || \
   ! openssl x509 -checkend 86400 -noout -in "${MP_SPDZ_DIR}/Player-Data/P0.pem" &>/dev/null; then
    log_progress "Generating fresh SSL certificates for ${max_parties} parties..."
    ( cd "$MP_SPDZ_DIR" && bash Scripts/setup-ssl.sh "$max_parties" && cd Player-Data && c_rehash . )
else
    log_progress "SSL certificates are valid."
fi

# ---------------------------------------------------------------------------
# Pre-compile all needed schedules before the benchmark loop.
# Compiling inside the timed section would inflate wall-clock measurements.
# ---------------------------------------------------------------------------
log_progress "Pre-compiling MPC schedules..."

# Ensure the auction_n_ring.mpc symlink exists for replicated-ring compilation.
# replicated-ring requires -R 64 at compile time; we use a distinct program name
# so the non-ring and ring schedules don't overwrite each other.
RING_SRC="${MP_SPDZ_DIR}/Programs/Source/auction_n_ring.mpc"
AUCTION_SRC="${MP_SPDZ_DIR}/Programs/Source/auction_n.mpc"
if [[ ! -e "$RING_SRC" ]]; then
    log_progress "Creating symlink: Programs/Source/auction_n_ring.mpc -> auction_n.mpc"
    ln -s "auction_n.mpc" "$RING_SRC"
fi

for protocol in $BENCH_PROTOCOLS; do
    for n in $BENCH_PARTIES; do
        # rep-ring only supports N=3
        if is_ring_protocol "$protocol" && [[ "$n" -ne 3 ]]; then
            continue
        fi

        # Per-protocol party count caps
        case "$protocol" in
            mascot) [[ "$n" -gt "$BENCH_MAX_PARTIES_MASCOT" ]] && continue ;;
            shamir) [[ "$n" -gt "$BENCH_MAX_PARTIES_SHAMIR" ]] && continue ;;
        esac

        if is_ring_protocol "$protocol"; then
            sched="${MP_SPDZ_DIR}/Programs/Schedules/auction_n_ring-${n}.sch"
            if [[ -f "$sched" ]]; then
                log_progress "  Already compiled: auction_n_ring-${n}.sch"
            else
                log_progress "  Compiling auction_n_ring N=${n} (rep-ring, -R 64)..."
                ( cd "$MP_SPDZ_DIR" && python3 compile.py -R 64 auction_n_ring -- "$n" )
                log_progress "  Done: auction_n_ring-${n}.sch"
            fi
        else
            sched="${MP_SPDZ_DIR}/Programs/Schedules/auction_n-${n}.sch"
            if [[ -f "$sched" ]]; then
                log_progress "  Already compiled: auction_n-${n}.sch"
            else
                log_progress "  Compiling auction_n N=${n} (${protocol})..."
                ( cd "$MP_SPDZ_DIR" && python3 compile.py auction_n -- "$n" )
                log_progress "  Done: auction_n-${n}.sch"
            fi
        fi
    done
done

log_progress "All schedules ready. Starting benchmark..."

# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------
for protocol in $BENCH_PROTOCOLS; do
    binary="$(protocol_binary "$protocol")"
    binary_path="${MP_SPDZ_DIR}/${binary}"

    if [[ ! -x "$binary_path" ]]; then
        log_warn "Binary not found or not executable: $binary_path — skipping protocol '$protocol'"
        continue
    fi

    for n in $BENCH_PARTIES; do
        # replicated-ring is hardwired to exactly 3 parties; skip other counts.
        if is_ring_protocol "$protocol" && [[ "$n" -ne 3 ]]; then
            log_progress "Skipping ${protocol} N=${n} (replicated-ring requires exactly 3 parties)"
            continue
        fi

        # Per-protocol party count caps (RAM constraints).
        case "$protocol" in
            mascot)
                if [[ "$n" -gt "$BENCH_MAX_PARTIES_MASCOT" ]]; then
                    log_progress "Skipping ${protocol} N=${n} (exceeds BENCH_MAX_PARTIES_MASCOT=${BENCH_MAX_PARTIES_MASCOT})"
                    continue
                fi ;;
            shamir)
                if [[ "$n" -gt "$BENCH_MAX_PARTIES_SHAMIR" ]]; then
                    log_progress "Skipping ${protocol} N=${n} (exceeds BENCH_MAX_PARTIES_SHAMIR=${BENCH_MAX_PARTIES_SHAMIR})"
                    continue
                fi ;;
        esac

        # Select the compiled program name.
        if is_ring_protocol "$protocol"; then
            prog_name="auction_n_ring-${n}"
        else
            prog_name="auction_n-${n}"
        fi

        for iter in $(seq 1 "$BENCH_ITERS"); do
            label="[${protocol} N=${n} iter ${iter}/${BENCH_ITERS}]"
            log_progress "${label} Starting..."

            # Port base for this iteration — offset by 100 per iteration to prevent
            # port collisions while previous iteration's TCP sockets are in TIME_WAIT.
            base_port=$(( BASE_PORT_START + (iter - 1) * 100 ))

            # ----------------------------------------------------------------
            # Build HOSTS file: N lines of 127.0.0.1
            # ----------------------------------------------------------------
            hosts_file="$(make_temp ".hosts")"
            for (( p=0; p<n; p++ )); do
                printf '127.0.0.1\n' >>"$hosts_file"
            done

            # ----------------------------------------------------------------
            # Assign bid values:
            #   Party 0 (seller / reserve price) = 0
            #   Parties 1..N-1 = random integer in [100, 10000]
            # ----------------------------------------------------------------
            declare -a bids=()
            bids+=( 0 )
            for (( p=1; p<n; p++ )); do
                bids+=( "$(random_bid)" )
            done

            # ----------------------------------------------------------------
            # Create per-party stdin, stderr capture files
            # ----------------------------------------------------------------
            declare -a stdin_files=()
            declare -a stderr_files=()
            for (( p=0; p<n; p++ )); do
                stdin_files+=( "$(make_temp ".p${p}.stdin")" )
                stderr_files+=( "$(make_temp ".p${p}.stderr")" )
            done

            # Write bid values to stdin files (avoids pipe which breaks stderr capture)
            for (( p=0; p<n; p++ )); do
                printf '%s\n' "${bids[$p]}" >"${stdin_files[$p]}"
            done

            # ----------------------------------------------------------------
            # Record wall-clock start, then spawn all N party processes
            # ----------------------------------------------------------------
            wall_start="$(wall_clock)"

            declare -a pids=()
            for (( p=0; p<n; p++ )); do
                # Build the argument list for this party.
                declare -a args=(
                    "$binary_path"
                    "-p" "$p"
                )
                # replicated-ring-party.x does NOT accept -N (party count is
                # implicit at compile time).  All other protocols need it.
                if ! is_ring_protocol "$protocol"; then
                    args+=( "-N" "$n" )
                fi
                args+=(
                    "-pn" "$base_port"   # port number base
                    "-ip" "$hosts_file"  # IP addresses file
                    "-I"                 # interactive: read input from stdin
                    "$prog_name"
                )

                # Spawn in a subshell that cd's to MP_SPDZ_DIR so that
                # all relative paths (Player-Data/, etc.) resolve correctly.
                # Use file redirect for stdin (not pipe) so stderr capture works.
                (
                    cd "$MP_SPDZ_DIR"
                    "${args[@]}" >/dev/null
                ) <"${stdin_files[$p]}" 2>"${stderr_files[$p]}" &

                pids+=( $! )
                unset args
            done

            # ----------------------------------------------------------------
            # Wait for all processes and collect exit codes
            # ----------------------------------------------------------------
            any_failed=0
            for (( p=0; p<n; p++ )); do
                wait "${pids[$p]}"
                ec=$?
                if [[ $ec -ne 0 ]]; then
                    log_warn "${label} Party ${p} exited with code ${ec}"
                    any_failed=1
                fi
            done

            wall_end="$(wall_clock)"

            if [[ "$any_failed" -ne 0 ]]; then
                log_warn "${label} One or more parties failed — skipping iteration."
                # Unset arrays before next iteration.
                unset bids pids stdin_files stderr_files
                declare -a bids=() pids=() stdin_files=() stderr_files=()
                continue
            fi

            # ----------------------------------------------------------------
            # Compute wall-clock duration
            # ----------------------------------------------------------------
            wall_secs="$(awk "BEGIN { printf \"%.3f\", $wall_end - $wall_start }")"

            # ----------------------------------------------------------------
            # Parse MP-SPDZ stats from party 0's stderr
            # ----------------------------------------------------------------
            read -r mpc_time data_mb rounds global_mb \
                <<<"$(parse_mpc_stats "${stderr_files[0]}")"

            # Ensure fields are never literally "unbound"
            mpc_time="${mpc_time:-}"
            data_mb="${data_mb:-}"
            rounds="${rounds:-}"
            global_mb="${global_mb:-}"

            # ----------------------------------------------------------------
            # Append CSV row
            # ----------------------------------------------------------------
            timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
            printf '%s\n' "${timestamp},${protocol},${n},${iter},${wall_secs},${mpc_time},${data_mb},${rounds},${global_mb}" \
                >>"$BENCH_OUT"

            log_progress "${label} Done — wall=${wall_secs}s mpc=${mpc_time}s data=${data_mb}MB rounds=${rounds} global=${global_mb}MB"

            # Unset arrays before the next iteration to avoid stale state.
            unset bids pids stdin_files stderr_files
            declare -a bids=() pids=() stdin_files=() stderr_files=()

            # Brief pause between iterations so TCP ports leave TIME_WAIT.
            sleep 2
        done

        # Brief pause between configurations.
        sleep 3
    done
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_progress "Benchmark complete."
printf '\n=== Results: %s ===\n' "$BENCH_OUT"
if command -v column &>/dev/null; then
    column -t -s',' "$BENCH_OUT"
else
    cat "$BENCH_OUT"
fi
