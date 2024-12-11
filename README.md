# Rafka

  Rafka is a blazing-fast, experimental distributed asynchronous message broker written in Rust. Inspired by Apache Kafka, it stands out with its peer-to-peer mesh architecture and custom in-memory database for unparalleled scalability and low-latency performance. Leveraging a P2P mesh inspired by Pastry and Chord, Rafka ensures hyper-scalability and efficient message routing. Each broker node features a built-in in-memory "sidecar" database for storing frequently accessed data, minimizing storage trips and optimizing responses for repetitive queries. Metadata management uses a distributed hash table (DHT) model for fault tolerance and seamless coordination. Designed for Kubernetes-native deployment, Rafka dynamically scales, avoids single points of failure, and excels in modern distributed environments.

### Current Status: Early Development

This project is in active development and **Not ready for production use**. 

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
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Overview

Rafka is a simple messaging system that allows producers to send messages to a broker, which then distributes them to consumers. The system is designed to be modular and extensible, with support for pluggable storage engines, hooks for custom logic, and configurable components.

This project serves as a learning tool for understanding how messaging systems work and how different components interact in a Rust-based application.

## Features

- **Broker Implementation**: Manages producers and consumers, dispatches messages, and integrates with storage engines and hooks.
- **Peer-Peer Mesh**: Creates a distributed layer around the system allowing for direct broker-broker communication in log(n) lookup time
- **Producers and Consumers**: Simple implementations that interact with the broker to send and receive messages.
- **Storage Engines**: Builtin In-memoryDB storage for persisting messages.
- **Asynchronous Processing**: Uses asynchronous programming with Tokio for high performance.

## Directory Structure

```bash
rafka/
├── Cargo.toml           # Workspace manifest
├── config/              # Configuration files
│   └── config.yaml
├── scripts/             # Tests and Examples       
│   ├── helloworld.sh         
│   ├── partition_demo.sh        
│   └── storage_demo.sh        
├── src/                 # Main binary crate
│   └── main.rs
├── crates/              # Directory for all crates
│   ├── core/            # Core types and traits
│   ├── broker/          # Broker implementation
│   ├── producer/        # Producer implementation
│   ├── consumer/        # Consumer implementation
│   ├── storage/         # Storage engines
└── 
```

## Getting Started

### Prerequisites

- **Rust**: Install Rust (latest stable version recommended) from [rustup.rs](https://rustup.rs/).
- **Cargo**: Comes with Rust installation.

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

- `Message`: Message Types and Handling.

### Broker

The `broker` crate contains the broker logic and handeling, topics and partition management.

### Producer

The `producer` crate provides the Producers, which buffers messages and sends them to the broker.

### Consumer

The `consumer` crate provides the Consumer, which subscribes to topics to receives messages from the broker.

### Storage

The `storage` crate provides implementations of storage engines:

## Rafka Development Checklist

#### Phase 1: Core Foundation
1. **Basic P2P Communication**
   - [ ] Implement node-to-node communication
   - [ ] Set up gRPC server and client
   - [x] Create basic message structures
   - [ ] Implement simple node discovery

2. **Message Handling**
   - [ ] Develop asynchronous message queue
   - [x] Implement basic producer/consumer logic
   - [x] Create basic message storage functionality
   - [ ] Set up error handling

#### Phase 2: Distributed Systems
1. **Consensus & Coordination**
   - [ ] Implement consensus algorithm
   - [ ] Develop leader election mechanism
   - [ ] Create cluster state management
   - [ ] Set up configuration sharing between nodes

2. **Data Management**
   - [ ] Implement message replication across nodes
   - [ ] Develop partition management logic
   - [ ] Create consistency checks for data integrity
   - [ ] Set up backup mechanisms

#### Phase 3: Production Readiness
1. **Kubernetes Integration**
   - [ ] Create basic Kubernetes deployment configuration
   - [ ] Implement StatefulSet for stable storage
   - [ ] Set up service discovery within Kubernetes
   - [ ] Develop auto-scaling logic for brokers and consumers

2. **Monitoring & Reliability**
   - [ ] Implement basic metrics collection
   - [ ] Set up health checks for brokers and services
   - [ ] Create monitoring dashboards (e.g., Prometheus, Grafana)
   - [ ] Define alerting rules for failures or bottlenecks

#### Phase 4: Performance & Security
1. **Performance Optimization**
   - [ ] Implement message batching for efficiency
   - [ ] Add message compression for reduced network load
   - [ ] Optimize network resource usage (e.g., reduce latency)
   - [ ] Create caching layer for frequently accessed data

2. **Security Implementation**
   - [ ] Enable TLS encryption for secure communication
   - [ ] Implement user authentication (OAuth2)
   - [ ] Set up authorization mechanisms (role-based access control)
   - [ ] Implement audit logging for security monitoring

#### Phase 5: Client SDK & Documentation
1. **Client Development**
   - [ ] Create a Rust client SDK for message production and consumption
   - [ ] Develop example applications using the SDK
   - [ ] Implement client-side monitoring (e.g., message processing time)
   - [ ] Create client documentation and setup guides

2. **Documentation**
   - [ ] Write comprehensive getting started guide
   - [ ] Create detailed API documentation
   - [ ] Develop deployment guide for both local and cloud environments
   - [ ] Add troubleshooting guide to help users resolve common issues

#### Additional Categories

1. **Network Resilience Strategies**
   - [ ] Implement network redundancy (multiple paths and regions)
   - [ ] Set up load balancing to distribute traffic evenly
   - [ ] Configure fault detection and self-healing mechanisms
   - [ ] Implement traffic shaping and prioritization (e.g., QoS, rate limiting)
   - [ ] Add retry logic with exponential backoff and circuit breakers

2. **High Availability & Scalability**
   - [ ] Ensure data and services are replicated across multiple nodes
   - [ ] Set up auto-scaling for brokers and consumers based on load
   - [ ] Configure dynamic partitioning to scale with traffic
   - [ ] Implement horizontal scaling for both producers and consumers


## Contributing

We welcome all contributions! The project is in very early stages, so there are many areas to help; listed in the checklist above.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- [Apache Kafka](https://kafka.apache.org) for inspiration on messaging systems.
- [Tokio](https://tokio.rs) for asynchronous runtime support.
- [@wyattgill9](https://github.com/wyattgill9) for the PoC.
- The Rust community for their excellent libraries and support.
