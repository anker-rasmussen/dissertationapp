#!/usr/bin/env bash
# run_veilid_bench.sh — Benchmark full auctions over the Veilid P2P network.
#
# Drives market-headless processes end-to-end: devnet setup, node launch,
# auction lifecycle, and BENCH: metric collection.
#
# Usage:
#   LD_PRELOAD=/path/to/libipspoof.so \
#   MP_SPDZ_DIR=/path/to/MP-SPDZ \
#   bash scripts/bench/run_veilid_bench.sh
#
# Environment variables:
#   BENCH_MODE              — "veilid" (vary parties) or "devnet_size" (vary devnet size)
#   BENCH_PARTIES           — space-separated party counts         (default: "3")
#   BENCH_DEVNET_SIZES      — space-separated devnet sizes         (default: "20")
#   BENCH_ITERS             — iterations per configuration         (default: 3)
#   BENCH_AUCTION_DURATION  — auction duration in seconds          (default: 30)
#   BENCH_OUT               — output CSV file                      (default: bench-results/veilid_auction.csv)
#   BENCH_WARMUP_SECS       — wait after devnet start/restart      (default: 30)
#   MARKET_BINARY           — path to market-headless binary       (auto-detected if empty)
#
# Required env vars (set by caller/Makefile):
#   LD_PRELOAD              — path to libipspoof.so
#   MP_SPDZ_DIR             — path to MP-SPDZ

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BENCH_MODE="${BENCH_MODE:-veilid}"
BENCH_PARTIES="${BENCH_PARTIES:-3 4 5 6 8 10}"
BENCH_DEVNET_SIZES="${BENCH_DEVNET_SIZES:-20}"
BENCH_ITERS="${BENCH_ITERS:-3}"
BENCH_AUCTION_DURATION="${BENCH_AUCTION_DURATION:-30}"
BENCH_OUT="${BENCH_OUT:-bench-results/veilid_auction.csv}"
BENCH_WARMUP_SECS="${BENCH_WARMUP_SECS:-30}"
MARKET_BINARY="${MARKET_BINARY:-}"

# ---------------------------------------------------------------------------
# Resolve script/project paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MARKET_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VEILID_DIR="$(cd "$MARKET_DIR/../../veilid" && pwd)"
COMPOSE_FILE="$VEILID_DIR/.devcontainer/compose/docker-compose.dev.yml"

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------
if [[ -z "${LD_PRELOAD:-}" ]]; then
    echo "ERROR: LD_PRELOAD must be set to the path of libipspoof.so" >&2
    exit 1
fi

if [[ -z "${MP_SPDZ_DIR:-}" ]]; then
    echo "ERROR: MP_SPDZ_DIR must be set to the path of the MP-SPDZ checkout" >&2
    exit 1
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo "ERROR: docker-compose file not found at $COMPOSE_FILE" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Resolve / build market-headless binary
# ---------------------------------------------------------------------------
resolve_binary() {
    if [[ -n "$MARKET_BINARY" ]]; then
        if [[ ! -x "$MARKET_BINARY" ]]; then
            echo "ERROR: MARKET_BINARY='$MARKET_BINARY' is not executable" >&2
            exit 1
        fi
        echo "$MARKET_BINARY"
        return
    fi

    local candidate="$MARKET_DIR/target/release/market-headless"
    if [[ -x "$candidate" ]]; then
        echo "$candidate"
        return
    fi

    log_progress "Building market-headless (release)..."
    ( cd "$MARKET_DIR" && cargo build --release --bin market-headless )
    if [[ ! -x "$candidate" ]]; then
        echo "ERROR: Build succeeded but binary not found at $candidate" >&2
        exit 1
    fi
    echo "$candidate"
}

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
log_progress() { echo "[$(date '+%H:%M:%S')] $*" >&2; }
log_warn()     { echo "[$(date '+%H:%M:%S')] WARNING: $*" >&2; }

# ---------------------------------------------------------------------------
# Temp resource tracking
# ---------------------------------------------------------------------------
declare -a _FIFOS=()
declare -a _TMPFILES=()
declare -a _NODE_PIDS=()

_cleanup() {
    for pid in "${_NODE_PIDS[@]+"${_NODE_PIDS[@]}"}"; do
        kill "$pid" 2>/dev/null || true
    done
    for f in "${_FIFOS[@]+"${_FIFOS[@]}"}" "${_TMPFILES[@]+"${_TMPFILES[@]}"}"; do
        rm -f -- "$f"
    done
}
trap _cleanup EXIT

# Clean stale fifos/data from killed previous runs
rm -f /tmp/bench_veilid_node* 2>/dev/null
rm -rf /tmp/market-headless-* 2>/dev/null

make_fifo() { rm -f "$1"; mkfifo "$1"; _FIFOS+=("$1"); }

make_temp() {
    local tmp; tmp="$(mktemp "/tmp/bench_veilid_XXXXXX${1:-}")"
    _TMPFILES+=("$tmp"); printf '%s' "$tmp"
}

# ---------------------------------------------------------------------------
# Devnet management
# ---------------------------------------------------------------------------

# wait_devnet_healthy <expected_nodes> <timeout_secs>
wait_devnet_healthy() {
    local expected="$1"
    local timeout="${2:-120}"
    log_progress "Waiting for $expected devnet node(s) to become healthy (timeout ${timeout}s)..."
    local i
    for i in $(seq 1 "$timeout"); do
        local healthy
        healthy=$(docker compose -f "$COMPOSE_FILE" ps --format "{{.Health}}" 2>/dev/null \
                  | grep -c healthy || true)
        if [[ "$healthy" -ge "$expected" ]]; then
            log_progress "Devnet ready: $healthy/$expected nodes healthy."
            return 0
        fi
        sleep 1
    done
    log_warn "Devnet not fully healthy after ${timeout}s (only $healthy/$expected). Proceeding anyway."
    return 0
}

# restart_devnet_with_size <desired_nodes>
# Restarts the devnet.  After startup, stops nodes beyond <desired_nodes>.
restart_devnet() {
    local desired="${1:-20}"
    log_progress "Restarting devnet (desired size: $desired)..."

    # Kill leftover market processes
    pkill -f market-headless 2>/dev/null || true
    pkill -f mascot-party.x  2>/dev/null || true

    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_FILE" up -d

    wait_devnet_healthy "$desired" 120

    # For devnet_size mode: stop nodes beyond the desired count.
    # The devnet always starts 20 nodes (veilid-dev-node-0 .. -19).
    if [[ "$desired" -lt 20 ]]; then
        log_progress "Stopping nodes $desired..19 to simulate smaller devnet..."
        local idx
        for idx in $(seq "$desired" 19); do
            docker compose -f "$COMPOSE_FILE" stop "veilid-dev-node-${idx}" 2>/dev/null || true
        done
    fi

    if [[ "$BENCH_WARMUP_SECS" -gt 0 ]]; then
        log_progress "Warming up devnet for ${BENCH_WARMUP_SECS}s..."
        sleep "$BENCH_WARMUP_SECS"
    fi
}

# ---------------------------------------------------------------------------
# Node IPC helpers
# ---------------------------------------------------------------------------

# send_cmd <fd> <json>
# Writes a JSON command line to the given file descriptor.
send_cmd() { printf '%s\n' "$2" >&"$1"; }

# wait_response <out_file> <line_number> <timeout_secs>
# Waits until <out_file> has at least <line_number> lines, then prints that line.
# Returns 1 on timeout.
wait_response() {
    local out_file="$1"
    local line_no="$2"
    local timeout="${3:-120}"
    local count=0
    while [[ $count -lt $timeout ]]; do
        local current
        current=$(wc -l < "$out_file" 2>/dev/null || echo 0)
        if [[ "$current" -ge "$line_no" ]]; then
            sed -n "${line_no}p" "$out_file"
            return 0
        fi
        sleep 1
        count=$(( count + 1 ))
    done
    return 1
}

# wait_for_bench_event <err_file> <event_name> <timeout_secs>
# Polls the stderr file for a BENCH: line with the given event name.
# Prints the matching line and returns 0, or returns 1 on timeout.
wait_for_bench_event() {
    local err_file="$1"
    local event="$2"
    local timeout="${3:-600}"
    local i
    for i in $(seq 1 "$timeout"); do
        if grep -q "\"event\":\"${event}\"" "$err_file" 2>/dev/null; then
            grep "\"event\":\"${event}\"" "$err_file" | tail -1
            return 0
        fi
        sleep 1
    done
    return 1
}

# extract_json_field <json_string> <field_name>
# Naively extracts a scalar field value from a JSON string using bash.
extract_json_field() {
    if [[ "$1" =~ \"${2}\":\"([^\"]+)\" ]]; then printf '%s' "${BASH_REMATCH[1]}"
    elif [[ "$1" =~ \"${2}\":([0-9]+\.?[0-9]*) ]]; then printf '%s' "${BASH_REMATCH[1]}"; fi
}

extract_bench_field() { extract_json_field "${1#*BENCH: }" "$2"; }

# ---------------------------------------------------------------------------
# wall_clock — seconds with fractional part
# ---------------------------------------------------------------------------
wall_clock() {
    if [[ -n "${EPOCHREALTIME+x}" ]]; then
        printf '%s' "$EPOCHREALTIME"
    else
        date +%s.%N
    fi
}

# ---------------------------------------------------------------------------
# run_auction_iteration <devnet_nodes> <num_parties> <iteration> <binary>
# ---------------------------------------------------------------------------
run_auction_iteration() {
    local devnet_nodes="$1"
    local num_parties="$2"
    local iter="$3"
    local binary="$4"

    local label="[devnet=${devnet_nodes} N=${num_parties} iter ${iter}/${BENCH_ITERS}]"
    log_progress "${label} Starting..."

    # Clean stale data dirs from previous iterations
    rm -rf /tmp/market-headless-* ~/.local/share/smpc-auction-* 2>/dev/null

    # Offsets: seller=20, bidders=21..20+N-1
    local seller_offset=20
    local -a offsets=()
    local i
    for i in $(seq 0 $(( num_parties - 1 ))); do
        offsets+=( $(( seller_offset + i )) )
    done

    # Per-node temp files
    local -a in_fifos=()
    local -a out_files=()
    local -a err_files=()
    local -a node_fds=()
    local -a node_pids=()

    for i in $(seq 0 $(( num_parties - 1 ))); do
        local off="${offsets[$i]}"
        local fifo="/tmp/bench_veilid_node${off}_in_$$"
        local out_file
        local err_file
        out_file="$(make_temp ".node${off}.out")"
        err_file="$(make_temp ".node${off}.err")"

        make_fifo "$fifo"
        in_fifos+=("$fifo")
        out_files+=("$out_file")
        err_files+=("$err_file")
    done

    # Launch all nodes
    for i in $(seq 0 $(( num_parties - 1 ))); do
        local off="${offsets[$i]}"
        local fifo="${in_fifos[$i]}"
        local out_file="${out_files[$i]}"
        local err_file="${err_files[$i]}"

        VEILID_NODE_OFFSET="$off" \
        MP_SPDZ_DIR="$MP_SPDZ_DIR" \
        LD_PRELOAD="$LD_PRELOAD" \
            "$binary" --offset "$off" \
            < "$fifo" \
            > "$out_file" \
            2> "$err_file" &

        local pid=$!
        node_pids+=("$pid")
        _NODE_PIDS+=("$pid")
    done

    # Open write ends of fifos (keep them open so the process doesn't get EOF prematurely)
    for i in $(seq 0 $(( num_parties - 1 ))); do
        local fifo="${in_fifos[$i]}"
        local fd=
        exec {fd}>"$fifo"
        node_fds+=("$fd")
    done

    # ---- Wait for all Ready events ----
    log_progress "${label} Waiting for ready events..."
    for i in $(seq 0 $(( num_parties - 1 ))); do
        local off="${offsets[$i]}"
        local out_file="${out_files[$i]}"
        local resp
        if ! resp="$(wait_response "$out_file" 1 120)"; then
            log_warn "${label} Node offset=${off} did not emit ready event within 120s"
            _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
            return 1
        fi
        log_progress "${label} Node ${off} ready: $resp"
    done

    # ---- WaitForRoutes on all nodes ----
    log_progress "${label} Sending WaitForRoutes..."
    local wait_routes_cmd
    wait_routes_cmd='{"cmd":"WaitForRoutes","timeout_secs":30}'
    for i in $(seq 0 $(( num_parties - 1 ))); do
        send_cmd "${node_fds[$i]}" "$wait_routes_cmd"
    done

    for i in $(seq 0 $(( num_parties - 1 ))); do
        local off="${offsets[$i]}"
        local resp
        if ! resp="$(wait_response "${out_files[$i]}" 2 60)"; then
            log_warn "${label} Node offset=${off} WaitForRoutes timed out"
            _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
            return 1
        fi
        if [[ "$resp" != *'"status":"Ok"'* ]]; then
            log_warn "${label} Node offset=${off} WaitForRoutes failed: $resp"
            _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
            return 1
        fi
        log_progress "${label} Node ${off} routes ready."
    done

    # ---- Seller creates listing ----
    log_progress "${label} Creating listing (seller offset=${seller_offset})..."
    local create_cmd
    create_cmd="{\"cmd\":\"CreateListing\",\"title\":\"Bench Item ${iter}\",\"content\":\"benchmark test\",\"reserve_price\":100,\"duration_secs\":${BENCH_AUCTION_DURATION}}"
    send_cmd "${node_fds[0]}" "$create_cmd"

    local create_resp
    if ! create_resp="$(wait_response "${out_files[0]}" 3 300)"; then
        log_warn "${label} CreateListing timed out"
        _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
        return 1
    fi
    if [[ "$create_resp" != *'"status":"Ok"'* ]]; then
        log_warn "${label} CreateListing failed: $create_resp"
        _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
        return 1
    fi

    local listing_key
    listing_key="$(extract_json_field "$(extract_json_field "$create_resp" "data")" "listing_key")"
    # Fallback: parse directly
    if [[ -z "$listing_key" ]]; then
        if [[ "$create_resp" =~ \"listing_key\":\"([^\"]+)\" ]]; then
            listing_key="${BASH_REMATCH[1]}"
        fi
    fi
    if [[ -z "$listing_key" ]]; then
        log_warn "${label} Could not parse listing_key from: $create_resp"
        _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
        return 1
    fi
    log_progress "${label} Listing created: $listing_key"

    # ---- Bidders place bids ----
    log_progress "${label} Placing bids from ${num_parties} bidders..."
    # Track response line numbers per node (seller already sent 3 lines)
    local -a next_line=()
    next_line+=(4)  # seller's next expected response line
    for i in $(seq 1 $(( num_parties - 1 ))); do
        next_line+=(3)  # bidders: ready(1) + routes(2) + bid-response(3)
    done

    for i in $(seq 1 $(( num_parties - 1 ))); do
        local off="${offsets[$i]}"
        local bid_amount=$(( RANDOM % 9901 + 100 ))
        local bid_cmd
        bid_cmd="{\"cmd\":\"PlaceBid\",\"listing_key\":\"${listing_key}\",\"amount\":${bid_amount}}"
        send_cmd "${node_fds[$i]}" "$bid_cmd"
        log_progress "${label} Node ${off} bidding ${bid_amount}..."
    done

    for i in $(seq 1 $(( num_parties - 1 ))); do
        local off="${offsets[$i]}"
        local resp
        if ! resp="$(wait_response "${out_files[$i]}" "${next_line[$i]}" 300)"; then
            log_warn "${label} Node ${off} PlaceBid response timed out"
        else
            if [[ "$resp" != *'"status":"Ok"'* ]]; then
                log_warn "${label} Node ${off} PlaceBid failed: $resp"
            else
                log_progress "${label} Node ${off} bid placed."
            fi
        fi
    done

    # ---- Wait for auction completion (poll seller's BENCH: lines) ----
    log_progress "${label} Waiting for auction to complete (up to $(( BENCH_AUCTION_DURATION + 600 ))s)..."
    local auction_timeout=$(( BENCH_AUCTION_DURATION + 600 ))
    local bench_line
    if ! bench_line="$(wait_for_bench_event "${err_files[0]}" "auction_complete" "$auction_timeout")"; then
        log_warn "${label} auction_complete event not seen within ${auction_timeout}s — aborting."
        _abort_iteration "${label}" "${node_pids[@]+"${node_pids[@]}"}"
        return 1
    fi
    log_progress "${label} Auction complete: $bench_line"

    # ---- Parse BENCH: metrics from seller stderr ----
    local seller_err="${err_files[0]}"

    local total_secs route_exchange_secs mpc_wall_secs mpc_self_secs
    local data_sent_mb rounds global_data_mb tunnel_bytes_sent tunnel_bytes_recv

    # auction_complete line
    total_secs="$(extract_bench_field "$bench_line" "total_secs")"

    # mpc_complete line
    local mpc_line
    mpc_line="$(grep '"event":"mpc_complete"' "$seller_err" | tail -1 || true)"
    mpc_wall_secs="$(extract_bench_field "$mpc_line" "mpc_wall_secs")"
    mpc_self_secs="$(extract_bench_field "$mpc_line" "mpc_self_secs")"
    data_sent_mb="$(extract_bench_field "$mpc_line" "data_sent_mb")"
    rounds="$(extract_bench_field "$mpc_line" "rounds")"
    global_data_mb="$(extract_bench_field "$mpc_line" "global_data_mb")"
    tunnel_bytes_sent="$(extract_bench_field "$mpc_line" "tunnel_bytes_sent")"
    tunnel_bytes_recv="$(extract_bench_field "$mpc_line" "tunnel_bytes_recv")"

    # route_exchange phase line
    local re_line
    re_line="$(grep '"event":"phase","name":"route_exchange"' "$seller_err" | tail -1 || true)"
    route_exchange_secs="$(extract_bench_field "$re_line" "secs")"

    # ---- Try GetDecryptionKey to confirm winner received key ----
    send_cmd "${node_fds[0]}" "{\"cmd\":\"GetDecryptionKey\",\"listing_key\":\"${listing_key}\"}"
    local key_resp
    key_resp="$(wait_response "${out_files[0]}" 4 60)" || key_resp=""
    if [[ "$key_resp" == *'"status":"Ok"'* ]]; then
        log_progress "${label} Seller confirmed decryption key available."
    else
        log_warn "${label} GetDecryptionKey: $key_resp"
    fi

    # ---- Shutdown all nodes ----
    log_progress "${label} Shutting down nodes..."
    for i in $(seq 0 $(( num_parties - 1 ))); do
        send_cmd "${node_fds[$i]}" '{"cmd":"Shutdown"}' || true
    done

    # Close write fds
    for fd in "${node_fds[@]+"${node_fds[@]}"}"; do
        exec {fd}>&- 2>/dev/null || true
    done

    # Wait for processes
    for pid in "${node_pids[@]+"${node_pids[@]}"}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Remove iteration pids from global tracking
    local -a remaining=()
    for pid in "${_NODE_PIDS[@]+"${_NODE_PIDS[@]}"}"; do
        local found=0
        for npid in "${node_pids[@]+"${node_pids[@]}"}"; do
            [[ "$pid" == "$npid" ]] && found=1 && break
        done
        [[ $found -eq 0 ]] && remaining+=("$pid")
    done
    _NODE_PIDS=("${remaining[@]+"${remaining[@]}"}")

    # ---- Append CSV row ----
    local timestamp
    timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf '%s\n' "${timestamp},${devnet_nodes},${num_parties},${iter},${total_secs:-},${route_exchange_secs:-},${mpc_wall_secs:-},${mpc_self_secs:-},${data_sent_mb:-},${rounds:-},${global_data_mb:-},${tunnel_bytes_sent:-},${tunnel_bytes_recv:-}" \
        >> "$BENCH_OUT"

    log_progress "${label} Done — total=${total_secs}s re=${route_exchange_secs}s mpc_wall=${mpc_wall_secs}s mpc_self=${mpc_self_secs}s data=${data_sent_mb}MB rounds=${rounds}"
    return 0
}

# _abort_iteration <label> [pid ...]
# Kills node processes for a failed iteration.
_abort_iteration() {
    local label="$1"
    shift
    for pid in "$@"; do kill "$pid" 2>/dev/null || true; done
    log_warn "${label} Iteration aborted."
}

# ---------------------------------------------------------------------------
# Ensure output directory and write CSV header
# ---------------------------------------------------------------------------
mkdir -p "$(dirname "$BENCH_OUT")"

CSV_HEADER="timestamp,devnet_nodes,num_parties,iteration,total_secs,route_exchange_secs,mpc_wall_secs,mpc_self_secs,data_sent_mb,rounds,global_data_mb,tunnel_bytes_sent,tunnel_bytes_recv"
if [[ ! -f "$BENCH_OUT" || ! -s "$BENCH_OUT" ]]; then
    printf '%s\n' "$CSV_HEADER" > "$BENCH_OUT"
fi

# ---------------------------------------------------------------------------
# Resolve binary (build once)
# ---------------------------------------------------------------------------
MARKET_BIN="$(resolve_binary)"
log_progress "Using binary: $MARKET_BIN"

# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------

if [[ "$BENCH_MODE" == "devnet_size" ]]; then
    # Outer loop: devnet sizes; inner loop: party counts
    for devnet_size in $BENCH_DEVNET_SIZES; do
        restart_devnet "$devnet_size"
        for num_parties in $BENCH_PARTIES; do
            if [[ "$num_parties" -gt $(( devnet_size )) ]]; then
                log_warn "Skipping N=${num_parties}: more parties than devnet nodes (${devnet_size})"
                continue
            fi
            for iter in $(seq 1 "$BENCH_ITERS"); do
                run_auction_iteration "$devnet_size" "$num_parties" "$iter" "$MARKET_BIN" || true
                # Brief cooldown between iterations
                sleep 5
            done
        done
    done
else
    # Default "veilid" mode: start devnet once, vary party counts
    restart_devnet 20
    for num_parties in $BENCH_PARTIES; do
        for iter in $(seq 1 "$BENCH_ITERS"); do
            run_auction_iteration 20 "$num_parties" "$iter" "$MARKET_BIN" || true
            sleep 5
        done
    done
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_progress "Benchmark complete. Results: $BENCH_OUT"
printf '\n=== Results: %s ===\n' "$BENCH_OUT"
if command -v column &>/dev/null; then
    column -t -s',' "$BENCH_OUT"
else
    cat "$BENCH_OUT"
fi
