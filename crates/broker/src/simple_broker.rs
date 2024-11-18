use as_any::Downcast;
use async_trait::async_trait;
use my_core::errors::broker_error::BrokerError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use my_core::broker::Broker;
use my_core::message::Message;
use my_core::storage_engine::StorageEngine;

use crate::hooks_integration::{HookRegistry as BrokerHookRegistry, HookResultType};
use crate::request::Request;
use crate::response::Response;

// Type aliases for producer and consumer IDs
type ProducerId = String;
type ConsumerId = String;

/// `SimpleBroker` is a basic broker implementation.
/// It manages producers, consumers, and handles message routing and storage.
pub struct SimpleBroker<S> {
    storage: Arc<S>, // Storage engine for persisting messages
    producers: Arc<Mutex<HashMap<ProducerId, mpsc::Sender<Box<dyn Message>>>>>, // Active producers
    consumers: Arc<Mutex<HashMap<ConsumerId, mpsc::Sender<Box<dyn Message>>>>>, // Active consumers
    hook_registry: Arc<BrokerHookRegistry>, // Hooks for extensibility (e.g., logging, metrics)
}

impl<S> SimpleBroker<S> {
    /// Creates a new instance of `SimpleBroker`.
    /// 
    /// # Parameters
    /// - `storage`: The storage engine for message persistence.
    /// - `hook_registry`: Registry containing hooks for request and response lifecycle events.
    pub fn new(storage: Arc<S>, hook_registry: Arc<BrokerHookRegistry>) -> Self {
        Self {
            storage,
            producers: Arc::new(Mutex::new(HashMap::new())),
            consumers: Arc::new(Mutex::new(HashMap::new())),
            hook_registry,
        }
    }
}

impl<S> SimpleBroker<S>
where
    S: StorageEngine,
{
    /// Handles an incoming request by invoking hooks, processing the request, and invoking hooks again.
    ///
    /// # Workflow
    /// 1. Execute `OnRequestReceived` hooks to inspect or modify the request.
    /// 2. Process the request to generate a response.
    /// 3. Execute `OnRequestExecuted` hooks to inspect or modify the response.
    ///
    /// # Errors
    /// Returns a `BrokerError` if a hook halts processing or an issue arises during request handling.
    async fn handle_request(&self, request: Request) -> Result<Response, BrokerError> {
        // Execute pre-processing hooks
        for hook in &self.hook_registry.on_request_received_hooks {
            match hook.on_request_received(&request) {
                HookResultType::Continue => {} // Proceed normally
                HookResultType::StopProcessing => {
                    return Err(BrokerError::HookError("Processing stopped by hook".into()))
                }
                HookResultType::ModifyRequest(_) => {} // Placeholder for modified request usage
                HookResultType::Error(err) => return Err(BrokerError::HookError(err.message)),
                _ => {}
            }
        }

        // Main request processing
        let response = self.process_request(&request).await?;

        // Execute post-processing hooks
        for hook in &self.hook_registry.on_request_executed_hooks {
            match hook.on_request_executed(&request, &response) {
                HookResultType::Continue => {} // Proceed normally
                HookResultType::StopProcessing => {
                    return Err(BrokerError::HookError("Processing stopped by hook".into()))
                }
                HookResultType::ModifyResponse(_) => {} // Placeholder for modified response usage
                HookResultType::Error(err) => return Err(BrokerError::HookError(err.message)),
                _ => {}
            }
        }

        Ok(response)
    }

    /// Processes an incoming request and generates a response.
    /// 
    /// # Note
    /// This is a simple placeholder implementation. Customize this method to fit application logic.
    async fn process_request(&self, request: &Request) -> Result<Response, BrokerError> {
        // Echo request body as the response
        let response = Response::new(
            200,
            vec![("Content-Type".into(), "application/octet-stream".into())],
            request.body.clone(),
        );
        Ok(response)
    }
}

#[async_trait]
impl<S> Broker for SimpleBroker<S>
where
    S: StorageEngine + 'static,
{
    type ProducerId = ProducerId;
    type ConsumerId = ConsumerId;

    /// Registers a new producer with the broker.
    /// 
    /// # Workflow
    /// 1. Creates a message channel for the producer.
    /// 2. Spawns a task to listen for incoming messages and store them using the storage engine.
    ///
    /// # Errors
    /// Returns `BrokerError` if the producer is already registered.
    async fn register_producer(&self, producer_id: Self::ProducerId) -> Result<(), BrokerError> {
        let mut producers = self.producers.lock().await;
        if producers.contains_key(&producer_id) {
            return Err(BrokerError::RegistrationFailed(format!(
                "Producer '{}' is already registered",
                producer_id
            )));
        }

        // Create a channel for this producer
        let (tx, mut rx) = mpsc::channel(100);
        producers.insert(producer_id.clone(), tx);

        let storage = self.storage.clone();

        // Handle incoming messages from the producer
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                if let Err(err) = storage
                    .append(
                        if let Some(msg) = message
                            .as_any()
                            .downcast_ref::<<S as StorageEngine>::Msg>()
                            .cloned()
                        {
                            msg
                        } else {
                            panic!("Error, {:?} cannot be downcasted!", message)
                        },
                    )
                    .await
                {
                    eprintln!("Storage error: {:?}", err); // Log the storage error
                }
            }
        });

        Ok(())
    }

    /// Registers a new consumer with the broker.
    ///
    /// # Workflow
    /// 1. Creates a message channel for the consumer.
    ///
    /// # Errors
    /// Returns `BrokerError` if the consumer is already registered.
    async fn register_consumer(&self, consumer_id: Self::ConsumerId) -> Result<(), BrokerError> {
        let mut consumers = self.consumers.lock().await;
        if consumers.contains_key(&consumer_id) {
            return Err(BrokerError::RegistrationFailed(format!(
                "Consumer '{}' is already registered",
                consumer_id
            )));
        }

        // Create a channel for this consumer
        let (tx, _) = mpsc::channel(100);
        consumers.insert(consumer_id.clone(), tx);

        Ok(())
    }

    /// Dispatches a message to all registered consumers and stores it using the storage engine.
    ///
    /// # Workflow
    /// 1. Invokes pre-dispatch hooks (if implemented).
    /// 2. Stores the message using the storage engine.
    /// 3. Sends the message to all registered consumers.
    ///
    /// # Errors
    /// Returns `BrokerError` if storing the message or sending to consumers fails.
    async fn dispatch_message(&self, message: Box<dyn Message>) -> Result<(), BrokerError> {
        // Store the message in the storage engine
        self.storage
            .append(
                if let Some(msg) = message
                    .as_any()
                    .downcast_ref::<<S as StorageEngine>::Msg>()
                    .cloned()
                {
                    msg
                } else {
                    panic!("Error, {:?} cannot be downcasted!", message)
                },
            )
            .await?;

        // Dispatch the message to all consumers
        let consumers = self.consumers.lock().await;
        for (consumer_id, consumer_tx) in consumers.iter() {
            if let Err(e) = consumer_tx.send(message.clone()).await {
                eprintln!(
                    "Failed to send message to consumer '{}': {:?}",
                    consumer_id, e
                );
                return Err(BrokerError::DispatchFailed(format!(
                    "Failed to send to consumer '{}': {}",
                    consumer_id, e
                )));
            }
        }

        Ok(())
    }
}
