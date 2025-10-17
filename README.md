# Flink WAL Test

A PyFlink application for testing Write-Ahead Log (WAL) functionality with state manipulation. This project demonstrates how to create Flink savepoints, modify the underlying state files, and resume job execution from the modified state.

## Overview

The application:
1. Runs a word counting job that uses ordered letter combinations (e.g., aaa, aab, aac, ..., zzz)
2. Creates a savepoint with the job's state
3. Modifies the savepoint's state files using a custom C++ tool
4. Resumes the job from the modified savepoint

This is useful for testing Flink's resilience and state recovery mechanisms when the underlying state has been manipulated.

## Prerequisites

- Docker
- At least 4GB of available memory for the Flink cluster

## Quick Start

### Build the Docker Image

```bash
docker build . -t flink-wal-test
```

### Run the Application

```bash
docker run --rm -v "$(pwd)/logs:/opt/flink/log" flink-wal-test
```

This command will:
- Start a Flink cluster inside the container
- Submit the PyFlink word counting job
- Create a savepoint when the job is running
- Modify the savepoint using the custom swap tool
- Resume the job from the modified savepoint
- Mount the logs directory so you can view Flink logs from the host

## Project Structure

```
.
├── Dockerfile              # Main Docker configuration
├── job.py                 # PyFlink word counting application
├── run.sh                 # Main execution script
├── swap_sst_last5.cpp     # C++ tool for modifying state files
├── config.yaml            # Flink configuration
├── pyproject.toml         # Python dependencies
└── logs/                  # Flink logs (created when running)
```

## What the Application Does

### Word Counter Job (`job.py`)
- Generates 20,000 words from ordered letter combinations
- By default uses 3-letter combinations (aaa, aab, aac, ..., zzz) - 17,576 total combinations
- Each combination appears approximately 1.1 times on average (20,000 words ÷ 17,576 combinations)
- Can be configured to use 4-letter combinations (456,976 total) for sparser distribution
- Counts occurrences of each combination using Flink's keyed state
- Uses checkpointing every 1000ms for fault tolerance

### State Manipulation (`run.sh` + `swap_sst_last5`)
1. **Job Submission**: Starts the PyFlink job in detached mode
2. **Savepoint Creation**: Creates a native savepoint once the job is running
3. **State Modification**: Uses the `swap_sst_last5` C++ tool to modify the largest state file
4. **Job Resume**: Restarts the job from the modified savepoint

## Viewing Logs

After running the container, you can view the Flink logs in the `logs/` directory on your host machine:

```bash
# View JobManager logs
tail -f logs/flink-*-jobmanager-*.log

# View TaskManager logs  
tail -f logs/flink-*-taskmanager-*.log
```

## Configuration

The Flink cluster configuration is defined in `config.yaml`. Key settings include:
- Checkpointing and savepoint directories
- RocksDB state backend configuration
- Memory and parallelism settings

## Development

### Local Development Setup

If you want to modify the code locally:

```bash
# Install dependencies with uv
uv sync

# Run the application locally (requires Flink installation)
./run.sh .venv/bin/python
```

### Building Custom State Manipulation Tool

The `swap_sst_last5.cpp` file contains the logic for modifying RocksDB state files. If you need to modify this tool, ensure you have the necessary build dependencies installed in the Docker image.

## Troubleshooting

### Common Issues

1. **Out of Memory**: Ensure Docker has at least 4GB of memory allocated
2. **Job Fails to Start**: Check the logs in the `logs/` directory for detailed error messages
3. **Savepoint Creation Fails**: Verify the job reaches RUNNING state before savepoint creation

### Debug Mode

To run with more verbose output, you can modify the logging levels in `config.yaml` or check the mounted log files for detailed information.

## Technical Details

- **Base Image**: `flink:latest`
- **Python Version**: 3.9.23 (managed by uv)
- **State Backend**: RocksDB with custom configuration
- **Checkpointing**: AT_LEAST_ONCE mode with 1-second intervals
- **Word Generation**: Ordered letter combinations (configurable 3 or 4 letters)
- **Dependency Management**: uv for fast Python package management

## License

This is a research/testing project for Flink WAL functionality.