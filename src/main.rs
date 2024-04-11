use nats::jetstream::{self, JetStream, ConsumerConfig, StreamConfig, DeliverPolicy, AckPolicy};
use chrono;
use std::{error::Error, sync::Arc};
use serde_json;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag="type")]
enum Event {
    DeprovisionRequested(DeprovisionRequestedEvent),
    Deprovisioned(DeprovisionedEvent),
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestDeprovisioning {
    organization_id: String,
    deprovisioning_id: String,
}


#[derive(Serialize, Deserialize, Debug)]
struct DeprovisionRequestedEvent {
    organization_id: String,
    deprovisioning_id: String,
    requested_at: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct DeprovisionedEvent {
    organization_id: String,
    deprovisioning_id: String,
    deprovisioned_at: String,
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nats_client = nats::connect("nats://localhost:4222")?;
    let js = nats::jetstream::new(nats_client);

    // ensure the stream exists for deprovision commands
    js.add_stream(StreamConfig {
        name: "DEPROVISION_COMMANDS".to_string(),
        subjects: vec!["deprovisioning.command.*".to_string()],
        ..Default::default()
    })?;

    // Ensure the stream exists for our deprovision events.
    js.add_stream(StreamConfig {
        name: "DEPROVISION_EVENTS".to_string(),
        subjects: vec!["deprovisioning.events.*".to_string()],
        ..Default::default()
    })?;

    // Simple consumer subscription.
    let command_consumer = ConsumerConfig {
        durable_name: Some("deprovision_commands_durable".to_string()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        deliver_subject: Some("DeprovisionCommands.handler".to_string()),
        ..Default::default()
    };

    let event_consumer = ConsumerConfig {
        durable_name: Some("deprovision_events_durable".to_string()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        deliver_subject: Some("DeprovisionEvents.handler".to_string()),
        ..Default::default()
    };

    js.add_consumer("DEPROVISION_COMMANDS", command_consumer)?;
    js.add_consumer("DEPROVISION_EVENTS", event_consumer)?;

    let js_clone = js.clone();

    let events_handle = tokio::spawn(async move {
        subscribe_to_events(js.clone()).await.unwrap();
    });

    let commands_handle = tokio::spawn(async move {
        subscribe_to_commands(js_clone).await.unwrap();
    });

    let _ = tokio::try_join!(events_handle, commands_handle);
    println!("Threads rejoined, I guess we're done here.");
    Ok(())
}

async fn subscribe_to_events(js: JetStream) -> Result<(), Box<dyn Error>> {
    let sub = js.subscribe("deprovisioning.events.*")?;

    loop {
        for msg in sub.timeout_iter(std::time::Duration::from_secs(1)) {
            println!("Received event msg: {:?}", msg);

            match serde_json::from_slice::<Event>(&msg.data) {
                Ok(Event::DeprovisionRequested(event)) => {
                    println!("Received event: {:?}", event);

                    handle_deprovision_requested(&js, event).await?;

                    msg.ack()?;
                },
                Ok(Event::Deprovisioned(event)) => {
                    println!("Received event: {:?}", event);

                    // No-op, we're done.
                    msg.ack()?;
                },
                Err(e) => {
                    eprintln!("Error deserializing event: {}", e);
                    // Decide how you want to handle this error.
                    // It might include logging it, sending a nack, etc.
                }
            }

        };
        println!("No event messages, continuing to listen...");
    }
}

async fn subscribe_to_commands(js: JetStream) -> Result<(), Box<dyn Error>> {
    let sub = js.subscribe("deprovisioning.command.*")?;

    loop {
        for msg in sub.timeout_iter(std::time::Duration::from_secs(1)) {
            println!("Received command msg: {:?}", msg);

            match serde_json::from_slice::<RequestDeprovisioning>(&msg.data) {
                Ok(command) => {
                    println!("Received command: {:?}", command);

                    process_deprovision_command(&js, command).await?;

                    msg.ack()?;
                },
                Err(e) => {
                    eprintln!("Error deserializing command: {}", e);
                    // Decide how you want to handle this error.
                    // It might include logging it, sending a nack, etc.
                }
            }

        };
        println!("No command messages, continuing to listen...");
    }
}


async fn process_deprovision_command(js: &JetStream, command: RequestDeprovisioning) -> Result<(), Box<dyn Error>> {
    // Process the command
    let response_event = DeprovisionRequestedEvent {
        organization_id: command.organization_id,
        deprovisioning_id: command.deprovisioning_id.clone(),
        requested_at: chrono::Utc::now().to_rfc3339(),
    };
    let wrapped_event = Event::DeprovisionRequested(response_event);

    let subject = format!("deprovisioning.events.{:?}", command.deprovisioning_id).to_string();

    let event_data = serde_json::to_vec(&wrapped_event)?;

    js.publish(&subject, event_data)?;
    println!("Published event: {:?}", wrapped_event);
    Ok(())
}

async fn handle_deprovision_requested(js: &JetStream, event: DeprovisionRequestedEvent) -> Result<(), Box<dyn Error>> {
    println!("Handling deprovision requested event: {:?}", event);
    let id = event.deprovisioning_id.clone();
    // Simulate some work

    let projection = project_deprovisioning(js, id).await?;
    if projection {
        return Ok(());
    }

    let response_event = DeprovisionedEvent {
        organization_id: event.organization_id,
        deprovisioning_id: event.deprovisioning_id.clone(),
        deprovisioned_at: chrono::Utc::now().to_rfc3339(),
    };

    let wrapped_event = Event::Deprovisioned(response_event);

    let subject = format!("deprovisioning.events.{:?}", event.deprovisioning_id).to_string();

    let event_data = serde_json::to_vec(&wrapped_event)?;

    js.publish(&subject, event_data)?;
    println!("Published event: {:?}", wrapped_event);
    Ok(())
}


async fn project_deprovisioning(js: &JetStream, id: String) -> Result<bool, Box<dyn Error>> {
    println!("Projecting deprovisioning: {:?}", id);

    let subject_pattern = format!("deprovisioning.events.{:?}", id).to_string();
    let events = fetch_deprovisioning_events(js, subject_pattern).await?;

    let mut deprovisioned = false;

    for event in events {
        match event {
            Event::DeprovisionRequested(_) => {
                // No-op, we're waiting for the deprovisioned event.
            },
            Event::Deprovisioned(_) => {
                deprovisioned = true;
                break;
            }
        }
    }

    if deprovisioned {
        println!("Deprovisioning complete for: {:?}", id);
    } else {
        println!("Deprovisioning not yet complete for: {:?}", id);
    }

    return Ok(deprovisioned)
}

async fn fetch_deprovisioning_events(js: &JetStream, subject_pattern: String) -> Result<Vec<Event>, Box<dyn Error>> {
    let sub = js.subscribe(&subject_pattern)?;

    let mut events = vec![];

    for msg in sub.timeout_iter(std::time::Duration::from_secs(1)) {
        match serde_json::from_slice::<Event>(&msg.data) {
            Ok(event) => {
                events.push(event);
            },
            Err(e) => {
                eprintln!("Error deserializing event: {}", e);
                // Decide how you want to handle this error.
                // It might include logging it, sending a nack, etc.
            }
        }
    }

    Ok(events)
}