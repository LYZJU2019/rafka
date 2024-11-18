use async_trait::async_trait;
use my_core::broker::Broker;
use my_core::consumer::Consumer;
use my_core::errors::consumer_error::ConsumerError;
use my_core::message::Message;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct SimpleConsumer<M, B>
where
    M: Message + 'static,
    B: Broker + Send + Sync + 'static,
{
    receiver: mpsc::Receiver<M>,
    // sender: mpsc::Sender<M>, // Add this line
    broker: Arc<B>,
    // consumer_id: B::ConsumerId,
}

impl<M, B> SimpleConsumer<M, B>
where
    M: Message + 'static,
    B: Broker + Send + Sync + 'static,
{
    pub async fn new(broker: Arc<B>, consumer_id: B::ConsumerId) -> Result<Self, ConsumerError> {
        let (tx, rx) = mpsc::channel(100);

        broker
            .register_consumer(consumer_id.clone())
            .await
            .map_err(|e| ConsumerError::BrokerError(e.to_string()))?;

        Ok(Self {
            receiver: rx,
            // sender: tx, // Add this line
            broker,
            // consumer_id,
        })
    }
}

#[async_trait]
impl<M, B> Consumer for SimpleConsumer<M, B>
where
    M: Message + 'static,
    B: Broker + Send + Sync + 'static,
{
    type Msg = M;

    async fn poll(&mut self) -> Result<Option<Self::Msg>, ConsumerError> {
        match self.receiver.recv().await {
            Some(msg) => Ok(Some(msg)),
            None => Ok(None),
        }
    }

    async fn commit(&self) -> Result<(), ConsumerError> {
        // Implement commit logic if necessary
        Ok(())
    }
}
