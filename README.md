# SAGA + Parsl Integration

Advanced task scheduling for Parsl using SAGA algorithms to control task-to-node placement in Kubernetes clusters.

## Project Structure

```
scheduling/                    # Main project repository
├── parsl/                    # Git submodule: Modified Parsl fork
├── saga/                     # Git submodule: SAGA scheduling library
├── scripts/                  # Test scripts and examples
│   ├── test_local.py        # Local test without K8s
│   └── test.py              # Full K8s integration test
├── pyproject.toml           # Main project configuration
└── README.md                # This file
```

## Overview

This project integrates SAGA (Scheduling Algorithms Gathered) with Parsl's High Throughput Executor to enable intelligent task placement on Kubernetes nodes based on:
- Task dependencies and DAG structure
- Network topology
- Computation and communication costs
- Scheduling algorithms (HEFT, CPOP, etc.)

### How It Works

1. **Task Graph Building**: Parsl builds a dependency graph of tasks
2. **SAGA Scheduling**: SAGA scheduler generates optimal task-to-node mapping
3. **Interchange Routing**: Custom routing in Parsl's interchange sends tasks to assigned nodes
4. **Node Matching**: Flexible hostname matching (exact, substring, or K8s node name)

## Setup

### Prerequisites

- Python 3.12+
- UV package manager
- Kubernetes cluster (for full integration tests)

### Installation

1. **Clone with submodules**:
```bash
git clone --recurse-submodules <your-repo-url>
cd scheduling
```

Or if you already cloned without submodules:
```bash
git submodule init
git submodule update
```

2. **Install with UV**:
```bash
uv sync
```

This installs both `parsl` and `saga` as editable local packages.

### Verifying Installation

```bash
uv run python -c "import parsl; import saga; print('✓ Ready!')"
```

## Running Tests

### Local Test (No K8s Required)

Tests SAGA scheduler integration with ThreadPoolExecutor:

```bash
uv run python scripts/test_local.py
```

Expected output:
- SAGA task graph built successfully
- Task assignments generated
- All 4 tasks assigned to nodes
- Tasks execute correctly

### Full Integration Test (Requires K8s)

Tests complete SAGA + Parsl + K8s integration with interchange routing:

```bash
uv run python scripts/test.py
```

Requires:
- Running Kubernetes cluster
- `default` namespace accessible
- Docker image `nick23447/parsl-worker:latest`

Expected log messages:
- `"SAGA: Task X assigned to node Y"`
- `"SAGA: Sent task X to manager Z on node Y"`

## Implementation Details

### Modified Files

**Parsl (`parsl/`):**
- `dataflow/dflow.py`: SAGA scheduler integration, task graph building, assignment storage
- `executors/high_throughput/interchange.py`: Two-phase routing with SAGA affinity
- `executors/high_throughput/process_worker_pool.py`: K8s node name reporting
- `executors/high_throughput/manager_record.py`: Added `k8s_node_name` field
- `config.py`: Added `saga_scheduler` configuration parameter

**SAGA (`saga/`):**
- No modifications needed - used as-is

### Key Design Decisions

1. **Works with Parsl's Architecture**: Routes tasks to existing worker pools (not per-task pods)
2. **Lazy Execution**: Explicitly call `dfk.execute()` to trigger SAGA scheduling
3. **Flexible Matching**: Matches SAGA node names to manager hostnames (exact/substring/k8s)
4. **HTEx Only**: SAGA affinity only applied to HighThroughputExecutor (skipped for ThreadPoolExecutor)

## Development

### Working on Submodules

To make changes to Parsl or SAGA:

```bash
# Navigate to submodule
cd parsl  # or cd saga

# Make changes, commit
git add .
git commit -m "Your changes"

# Push to your fork
git push

# Return to main repo and update submodule reference
cd ..
git add parsl  # or saga
git commit -m "Update parsl submodule"
```

### Adding New Scripts

Place all test scripts and examples in `scripts/`:

```bash
# Create new script
cat > scripts/my_test.py << 'EOF'
import parsl
from saga.schedulers.heft import HeftScheduler
# Your code here
EOF

# Run with UV
uv run python scripts/my_test.py
```

## Current Status

✅ Implementation complete
✅ Local testing successful
✅ SAGA scheduler integration working
⏳ Full K8s integration testing pending

## Next Steps

1. Set up K8s cluster for end-to-end testing
2. Add K8s Downward API to expose node names
3. Test interchange routing with multiple nodes
4. Benchmark performance vs default scheduling

## References

- [Parsl Documentation](https://parsl.readthedocs.io/)
- [SAGA GitHub](https://github.com/ANRGUSC/saga)
- [Modified Parsl Fork](https://github.com/kubishi/parsl)

## Contributing

This is a research project. For questions or contributions, please open an issue.

## License

- Parsl: Apache 2.0
- SAGA: MIT
- This integration: TBD
