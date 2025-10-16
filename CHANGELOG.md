# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.0.1] - Initial Release

### Added
- NATS transport implementation for Rebus message bus
- NATS saga storage implementation using key-value stores
- NATS subscription storage implementation using streams
- NATS outbox support using JetStream
- Async request/reply pattern using NATS pub/sub
- Support for scatter/gather messaging patterns
- Reply context for deferred responses in sagas
- Reply routing support for multi-server NATS configurations
- Support for client-only, host-only, or bidirectional async modes
