# Broadcast Service

## MVP

- [ ] Authentication
- [ ] Extend instrumentation (better traces)
- [ ] More integration tests
- [x] Broker trait - Support different brokers - Add a second Broker (HttpBroker)
- [?] Support binary messages (i.e. so we can use protobuf)
- [?] Make RedisBroker generic over message type (String is hardcoded)
- [x] Add configurable logging (done via `tracing` stdout)
- [x] Client-server example
- [x] Reconnect to redis when needed
- [x] Add cli options
- [x] Transform the admin commands onto enum/struct
- [x] Add statsd support
- [x] Add tracing
