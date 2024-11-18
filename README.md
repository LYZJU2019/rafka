# Rafka

An implementation of a simple message broker, producer, and consumer system in Rust, inspired by Apache Kafka. This project demonstrates how to integrate various components such as a broker, producer, consumer, storage engine, and hooks within a Rust-based asynchronous system.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Directory Structure](#directory-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Building the Project](#building-the-project)
  - [Running the Application](#running-the-application)
- [Components](#components)
  - [Core](#core)
  - [Broker](#broker)
  - [Producer](#producer)
  - [Consumer](#consumer)
  - [Storage](#storage)
  - [Hooks](#hooks)
- [Usage Example](#usage-example)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Overview

Rafka is a simple messaging system that allows producers to send messages to a broker, which then distributes them to consumers. The system is designed to be modular and extensible, with support for pluggable storage engines, hooks for custom logic, and configurable components.

This project serves as a learning tool for understanding how messaging systems work and how different components interact in a Rust-based application.

## Features

- **Broker Implementation**: Manages producers and consumers, dispatches messages, and integrates with storage engines and hooks.
- **Producers and Consumers**: Simple implementations that interact with the broker to send and receive messages.
- **Storage Engines**: In-memory and RocksDB storage engines for persisting messages.
- **Hooks**: Support for hooks that can modify requests and responses or execute custom logic at various stages.
- **Configurable Components**: Components that can be configured via configuration files.
- **Asynchronous Processing**: Uses asynchronous programming with Tokio for high performance.

## Directory Structure

```bash
rafka/
├── Cargo.toml           # Workspace manifest
├── config/              # Configuration files
│   └── config.yaml
├── src/                 # Main binary crate
│   └── main.rs
├── crates/              # Directory for all crates
│   ├── core/            # Core types and traits
│   ├── broker/          # Broker implementation
│   ├── producer/        # Producer implementation
│   ├── consumer/        # Consumer implementation
│   ├── storage/         # Storage engines
│   ├── hooks/           # Hooks and events
│   └── utils/           # Utility functions and common code
└── tests/               # Integration tests
```

## Getting Started

### Prerequisites

- **Rust**: Install Rust (latest stable version recommended) from [rustup.rs](https://rustup.rs/).
- **Cargo**: Comes with Rust installation.
- **RocksDB**: If you plan to use the RocksDB storage engine, ensure you have the necessary dependencies installed.

### Building the Project

Clone the repository:

```bash
git clone https://github.com/yourusername/rafka.git
cd rafka
```

Build the project using Cargo:

```bash
cargo build
```

### Running the Application

Run the application with:

```bash
cargo run
```

This will execute the main.rs file, which sets up the broker, producer, and consumer, and demonstrates sending and receiving messages.

## Components

### Core

The rafka_core crate defines the core traits and interfaces used throughout the system, including:

- `Broker`: Trait for broker implementations.
- `Producer`: Trait for producer implementations.
- `Consumer`: Trait for consumer implementations.
- `StorageEngine`: Trait for storage engine implementations.
- `Message`: Trait representing a message.

### Broker

The `broker` crate contains the SimpleBroker implementation, which manages producers and consumers, dispatches messages, and integrates with storage engines and hooks.

### Producer

The `producer` crate provides the SimpleProducer, which buffers messages and sends them to the broker.

### Consumer

The `consumer` crate provides the SimpleConsumer, which receives messages from the broker.

### Storage

The `storage` crate provides implementations of storage engines:

- `InMemoryStorage`: An in-memory storage engine with write-ahead log (WAL) and snapshot capabilities.
- `RocksDBStorage`: A storage engine using RocksDB for persistent storage.

### Hooks

The `hooks` crate defines hook traits and allows for custom logic to be executed at various stages of processing.

## Usage Example

The `src/main.rs` file demonstrates how to set up and run the system

## Configuration

Components can be configured via configuration files or programmatically. For example, the storage engine can be configured to use different storage types.

Example `config.yaml`:

```yaml
storage:
  storage_type: "rocksdb"
  path: "./data/rocksdb"
```

## Contributing

Contributions are welcome! Please open issues and submit pull requests for any features or bug fixes.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- [Apache Kafka](https://kafka.apache.org) for inspiration on messaging systems.
- [Tokio](https://tokio.rs) for asynchronous runtime support.
- [RocksDB](https://rocksdb.org) for storage capabilities.
- [@wyattgill9](https://github.com/wyattgill9) for the PoC.
- The Rust community for their excellent libraries and support.
