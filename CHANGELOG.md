# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.0.4] - 2025-12-04

### Added
- Durable subscription support

### Removed
- Saga storage support (not a good fit for NATS KV)
- Outbox support (incorrect implementation/no reason to keep without saga storage)

## [0.0.3] - 2025-11-09

### Changed
- Use `Rebus.Config` namespace for extensions, following convention used in other Rebus libraries

## [0.0.2] - 2025-10-17

### Changed
- Major transport refactor with native NATS subscriptions
- Use native inbox key creation
- Improved handling for orphaned saga index keys
- Optimize correlation property lookup

## [0.0.1] - 2025-10-16

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
