#!/bin/bash
# Integration test runner for the SMPC marketplace
#
# This script handles:
# 1. Starting the devnet Docker containers
# 2. Waiting for all nodes to become healthy
# 3. Cleanup of old test state
# 4. Running integration tests individually (each in separate process)
# 5. Shutting down the devnet when done
#
# Usage:
#   ./scripts/run-integration-tests.sh                    # Run all integration tests
#   ./scripts/run-integration-tests.sh dht                # Run only DHT tests
#   ./scripts/run-integration-tests.sh auction            # Run only auction tests
#   ./scripts/run-integration-tests.sh mpc                # Run only MPC tests
#
# Options:
#   --no-shutdown    Don't shut down devnet after tests (useful for debugging)
#   --no-start       Don't start devnet (assume it's already running)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VEILID_DIR="${VEILID_DIR:-/home/broadcom/Repos/Dissertation/Repos/veilid}"
COMPOSE_DIR="$VEILID_DIR/.devcontainer/compose"
COMPOSE_FILE="docker-compose.dev.yml"

# Options
SHUTDOWN_AFTER=true
START_DEVNET=true

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Cleanup old test state
cleanup_test_state() {
    echo_info "Cleaning up old test state..."
    rm -rf ~/.local/share/smpc-auction-node-*
    rm -rf /tmp/market-integration-tests
}

# Cleanup old Docker volumes
cleanup_docker_volumes() {
    echo_info "Cleaning up old Docker volumes..."
    local volumes
    volumes=$(docker volume ls --format "{{.Name}}" | grep veilid || true)
    if [ -n "$volumes" ]; then
        echo "$volumes" | xargs -r docker volume rm 2>/dev/null || true
    fi
}

# Start the devnet
start_devnet() {
    echo_step "Starting devnet..."

    if ! [ -d "$COMPOSE_DIR" ]; then
        echo_error "Compose directory not found: $COMPOSE_DIR"
        exit 1
    fi

    if ! [ -f "$COMPOSE_DIR/$COMPOSE_FILE" ]; then
        echo_error "Compose file not found: $COMPOSE_DIR/$COMPOSE_FILE"
        exit 1
    fi

    # Start containers
    (cd "$COMPOSE_DIR" && docker compose -f "$COMPOSE_FILE" up -d)

    echo_info "Devnet containers started"
}

# Wait for all devnet nodes to be healthy
wait_for_healthy() {
    echo_step "Waiting for devnet nodes to become healthy..."

    local max_attempts=60
    local attempt=0
    local expected_nodes=5  # bootstrap + 4 nodes

    while [ $attempt -lt $max_attempts ]; do
        local healthy_count
        healthy_count=$(docker ps --filter "name=veilid-dev" --filter "health=healthy" --format "{{.Names}}" | wc -l)

        if [ "$healthy_count" -ge "$expected_nodes" ]; then
            echo_info "All $expected_nodes devnet nodes are healthy"
            return 0
        fi

        attempt=$((attempt + 1))
        echo "  Waiting for nodes to be healthy ($healthy_count/$expected_nodes)... attempt $attempt/$max_attempts"
        sleep 2
    done

    echo_error "Timeout waiting for devnet nodes to become healthy"
    echo_info "Current container status:"
    docker ps --filter "name=veilid-dev" --format "table {{.Names}}\t{{.Status}}"
    return 1
}

# Check if devnet is running
check_devnet() {
    if ! docker ps --filter "name=veilid-dev-bootstrap" --format "{{.Names}}" | grep -q "veilid-dev-bootstrap"; then
        return 1
    fi
    return 0
}

# Stop the devnet
stop_devnet() {
    echo_step "Stopping devnet..."

    if [ -d "$COMPOSE_DIR" ] && [ -f "$COMPOSE_DIR/$COMPOSE_FILE" ]; then
        (cd "$COMPOSE_DIR" && docker compose -f "$COMPOSE_FILE" down) || true
    fi

    echo_info "Devnet stopped"
}

# Set up LD_PRELOAD for IP translation
setup_ld_preload() {
    local lib_path="$VEILID_DIR/.devcontainer/scripts/libipspoof.so"
    if [ ! -f "$lib_path" ]; then
        echo_warn "IP spoof library not found at: $lib_path"
        echo_info "Attempting to compile it..."

        local src_path="$VEILID_DIR/.devcontainer/scripts/ip_spoof.c"
        if [ -f "$src_path" ]; then
            gcc -shared -fPIC -o "$lib_path" "$src_path" -ldl
            echo_info "Compiled libipspoof.so"
        else
            echo_error "Cannot find ip_spoof.c at: $src_path"
            exit 1
        fi
    fi
    export LD_PRELOAD="$lib_path"
    echo_info "LD_PRELOAD set to: $LD_PRELOAD"
}

# Run a single integration test
run_test() {
    local test_file="$1"
    local test_name="$2"

    echo ""
    echo_info "Running: $test_file::$test_name"

    # Run each test as a separate cargo invocation to get a fresh process
    if cargo test --test "$test_file" "$test_name" -- --test-threads=1 2>&1; then
        echo_info "PASSED: $test_file::$test_name"
        return 0
    else
        echo_error "FAILED: $test_file::$test_name"
        return 1
    fi
}

# Get list of tests from a test file
get_tests() {
    local test_file="$1"
    cargo test --test "$test_file" --no-run 2>/dev/null
    cargo test --test "$test_file" -- --list 2>/dev/null | grep ": test$" | sed 's/: test$//'
}

# Cleanup handler for script exit
cleanup_on_exit() {
    local exit_code=$?

    if [ "$SHUTDOWN_AFTER" = true ]; then
        echo ""
        stop_devnet
        cleanup_docker_volumes
    else
        echo_info "Leaving devnet running (--no-shutdown specified)"
    fi

    exit $exit_code
}

# Parse command line options
parse_options() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --no-shutdown)
                SHUTDOWN_AFTER=false
                shift
                ;;
            --no-start)
                START_DEVNET=false
                shift
                ;;
            -h|--help)
                echo "Usage: $0 [OPTIONS] [FILTER]"
                echo ""
                echo "FILTER:"
                echo "  all       Run all integration tests (default)"
                echo "  dht       Run only DHT integration tests"
                echo "  auction   Run only auction flow tests"
                echo "  mpc       Run only MPC integration tests"
                echo ""
                echo "OPTIONS:"
                echo "  --no-shutdown   Don't shut down devnet after tests"
                echo "  --no-start      Don't start devnet (assume already running)"
                echo "  -h, --help      Show this help message"
                exit 0
                ;;
            -*)
                echo_error "Unknown option: $1"
                exit 1
                ;;
            *)
                # Not an option, must be the filter
                break
                ;;
        esac
    done
}

# Main execution
main() {
    cd "$PROJECT_DIR"

    # Parse options first
    parse_options "$@"

    # Shift past options to get filter
    while [[ $# -gt 0 ]] && [[ "$1" == --* ]]; do
        shift
    done

    local filter="${1:-all}"

    echo "=========================================="
    echo_info "SMPC Marketplace Integration Tests"
    echo "=========================================="
    echo ""

    # Set up cleanup trap
    trap cleanup_on_exit EXIT

    # Initial cleanup
    cleanup_test_state
    cleanup_docker_volumes

    # Start devnet if needed
    if [ "$START_DEVNET" = true ]; then
        if check_devnet; then
            echo_info "Devnet already running, restarting for clean state..."
            stop_devnet
            cleanup_docker_volumes
            sleep 2
        fi
        start_devnet
        wait_for_healthy
    else
        if ! check_devnet; then
            echo_error "Devnet is not running and --no-start was specified"
            exit 1
        fi
        echo_info "Using existing devnet (--no-start specified)"
    fi

    # Set up LD_PRELOAD
    setup_ld_preload

    # Give the network a moment to stabilize
    echo_info "Waiting for network to stabilize..."
    sleep 3

    local passed=0
    local failed=0

    case "$filter" in
        dht|all)
            echo ""
            echo_step "=== Running DHT Integration Tests ==="
            for test in $(get_tests dht_integration); do
                cleanup_test_state  # Clean between tests
                if run_test dht_integration "$test"; then
                    passed=$((passed + 1))
                else
                    failed=$((failed + 1))
                fi
            done
            ;;&
        auction|all)
            echo ""
            echo_step "=== Running Auction Flow Tests ==="
            for test in $(get_tests auction_flow); do
                cleanup_test_state
                if run_test auction_flow "$test"; then
                    passed=$((passed + 1))
                else
                    failed=$((failed + 1))
                fi
            done
            ;;&
        mpc|all)
            echo ""
            echo_step "=== Running MPC Integration Tests ==="
            for test in $(get_tests mpc_integration); do
                cleanup_test_state
                if run_test mpc_integration "$test"; then
                    passed=$((passed + 1))
                else
                    failed=$((failed + 1))
                fi
            done
            ;;
    esac

    echo ""
    echo "=========================================="
    if [ $failed -eq 0 ]; then
        echo_info "Test Results: $passed passed, $failed failed"
    else
        echo_error "Test Results: $passed passed, $failed failed"
    fi
    echo "=========================================="

    if [ $failed -gt 0 ]; then
        exit 1
    fi
}

main "$@"
