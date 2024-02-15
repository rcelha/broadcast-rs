# Broadcast Service

## MVP

- [ ] Authentication
- [ ] Broker trait - Support different brokers - Add a second Broker (HttpBroker)
- [ ] Support binary messages (i.e. so we can use protobuf)
- [x] Add cli options
- [x] Transform the admin commands onto enum/struct
- [x] Add statsd support
- [x] Add tracing
- [ ] Extend instrumentation (better traces)
- [x] Add configurable logging (done via `tracing` stdout)
- [x] Client-server example
- [ ] More integration tests
- [ ] Make RedisBroker generic over message type (String is hardcoded)?
