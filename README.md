# Deprovision Stuff

This is a demonstration application showing Event Sourcing in Rust using NATS JetStream as the event source.

## Command

    RequestDeprovisioning

## Events

    DeprovisioningRequested
    Deprovisioned

This uses a fairly crude projection system to, upon receiving the DeprovisoningRequested command, check to see if the "Deprovisioned" event has already been seen, and no-op.

# Usage

Install Nats and start it with jetstream enabled

Inject a RequestDeprovisioning command with the following command

    nats pub deprovisioning.command.request '{"organization_id": "123", "deprovisioning_id": "some-sixth-uuid"}'