use async_trait::async_trait;
use my_core::ProducerError;
use std::sync::Arc;
use tokio::sync::Mutex;

use my_core::broker::Broker;
use my_core::message::Message;
use my_core::producer::Producer;

/// A simple producer that sends messages to a broker.
///
/// The `SimpleProducer` buffers messages and sends them to the broker asynchronously.
pub struct SimpleProducer<M, B>
where
    B: Broker + Send + Sync + 'static,
{
    broker: Arc<B>,
    producer_id: B::ProducerId,
    buffer: Arc<Mutex<Vec<M>>>,
}

impl<M, B> SimpleProducer<M, B>
where
    B: Broker + Send + Sync + 'static,
{
    /// Creates a new `SimpleProducer` instance.
    ///
    /// # Arguments
    ///
    /// * `broker` - An `Arc` to the broker instance.
    /// * `producer_id` - The identifier for this producer.
    pub async fn new(broker: Arc<B>, producer_id: B::ProducerId) -> Result<Self, ProducerError> {
        // Register the producer with the broker
        broker
            .register_producer(producer_id.clone())
            .await
            .map_err(|e| ProducerError::BrokerError(e.to_string()))?;

        Ok(Self {
            broker,
            producer_id,
            buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

#[async_trait]
impl<M, B> Producer for SimpleProducer<M, B>
where
    M: Message + 'static,
    B: Broker + Send + Sync + 'static,
{
    type Msg = M;

    /// Sends a message to the broker.
    ///
    /// The message is buffered and sent asynchronously.
    async fn send(&self, message: Self::Msg) -> Result<(), ProducerError> {
        let mut buffer = self.buffer.lock().await;
        buffer.push(message);
        Ok(())
    }

    /// Flushes any buffered messages, ensuring they are sent.
    ///
    /// This method sends all buffered messages to the broker.
    async fn flush(&self) -> Result<(), ProducerError> {
        let mut buffer = self.buffer.lock().await;
        if buffer.is_empty() {
            return Ok(());
        }

        // Create a task to send messages to the broker
        let messages = std::mem::take(&mut *buffer);
        let broker = self.broker.clone();
        let _producer_id = self.producer_id.clone();

        for message in messages {
            // Wrap the message in a Box<dyn Message>
            let boxed_message = Box::new(message) as Box<dyn Message + Send + Sync>;

            // Dispatch the message to the broker
            broker
                .dispatch_message(boxed_message)
                .await
                .map_err(|e| ProducerError::BrokerError(e.to_string()))?;
        }

        Ok(())
    }
}
