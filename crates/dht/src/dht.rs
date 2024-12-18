use std::{error::Error, collections::HashSet, sync::Arc};
use futures::stream::StreamExt;
use libp2p::{
    kad::{self, store::MemoryStore, Mode, QueryResult},
    mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, PeerId, Multiaddr,
};
use tokio::sync::broadcast;

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: kad::Behaviour<MemoryStore>,
    mdns: mdns::tokio::Behaviour,
}

pub struct DHTNode {
    swarm: libp2p::Swarm<Behaviour>,
    peers: Arc<HashSet<PeerId>>,
    events_tx: broadcast::Sender<DHTEvent>,
}

#[derive(Clone, Debug)]
pub enum DHTEvent {
    PeerDiscovered(PeerId),
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
}

impl DHTNode {
    pub async fn new() -> Result<(Self, broadcast::Receiver<DHTEvent>), Box<dyn Error>> {
        let (events_tx, events_rx) = broadcast::channel(100);

        let mut swarm = libp2p::SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_behaviour(|key| {
                Ok(Behaviour {
                    kademlia: kad::Behaviour::new(
                        key.public().to_peer_id(),
                        MemoryStore::new(key.public().to_peer_id()),
                    ),
                    mdns: mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        key.public().to_peer_id(),
                    )?,
                })
            })?
            .build();

        swarm.behaviour_mut().kademlia.set_mode(None);
        
        Ok((Self {
            swarm,
            peers: Arc::new(HashSet::new()),
            events_tx,
        }, events_rx))
    }

    pub async fn start(&mut self, addr: Option<Multiaddr>) -> Result<(), Box<dyn Error>> {
        // Listen on provided address or default
        let listen_addr = addr.unwrap_or("/ip4/0.0.0.0/tcp/0".parse().unwrap());
        self.swarm.listen_on(listen_addr)?;

        loop {
            match self.swarm.select_next_some().await {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {}", address);
                }
                SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, addr) in list {
                        self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                        let _ = self.events_tx.send(DHTEvent::PeerDiscovered(peer_id));
                    }
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    let _ = self.events_tx.send(DHTEvent::PeerConnected(peer_id));
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    let _ = self.events_tx.send(DHTEvent::PeerDisconnected(peer_id));
                }
                _ => {}
            }
        }
    }

    pub fn get_peers(&self) -> Vec<PeerId> {
        self.peers.iter().cloned().collect()
    }

    pub async fn connect_to_peer(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error>> {
        self.swarm.dial(addr)?;
        Ok(())
    }
}

// Example usage for brokers
#[cfg(test)]
mod tests {
    use std::sync::Once;
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;
    use tracing::{info, error};
    use tracing_subscriber::EnvFilter;

    static START: Once = Once::new();
    fn init_logger() {
        START.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into())
                    .add_directive("rafka_dht=debug".parse().unwrap()))
                .init();
        });
    }

    #[tokio::test]
    async fn test_two_nodes_discovery() -> Result<(), Box<dyn Error>> {
        // Initialize tracing
        init_logger();

        // Create first node
        let (mut node1, mut events_rx1) = DHTNode::new().await?;
        info!("Created node1");
        
        // Start first node on a specific port
        let addr1 = "/ip4/127.0.0.1/tcp/0".parse()?;
        node1.swarm.listen_on(addr1)?;
        
        // Wait for listen address
        let listen_addr = wait_for_listen_addr(&mut node1.swarm).await?;
        info!("Node1 listening on {}", listen_addr);

        // Spawn node1's event loop
        let node1_handle = tokio::spawn(async move {
            if let Err(e) = node1.start(None).await {
                error!("Node1 error: {}", e);
            }
        });

        // Create second node
        let (mut node2, mut events_rx2) = DHTNode::new().await?;
        info!("Created node2");
        
        // Connect node2 to node1
        node2.connect_to_peer(listen_addr.clone()).await?;
        info!("Node2 connecting to {}", listen_addr);

        let node2_handle = tokio::spawn(async move {
            if let Err(e) = node2.start(None).await {
                error!("Node2 error: {}", e);
            }
        });

        // Wait for peer discovery
        let mut connected = false;
        timeout(Duration::from_secs(10), async {
            while let Ok(event) = events_rx1.recv().await {
                match event {
                    DHTEvent::PeerConnected(peer_id) => {
                        info!("Node1 connected to peer: {}", peer_id);
                        connected = true;
                        break;
                    }
                    event => info!("Node1 received event: {:?}", event),
                }
            }
        })
        .await
        .map_err(|_| "Timeout waiting for peer discovery")?;

        assert!(connected, "Peers failed to connect");

        // Clean up
        node1_handle.abort();
        node2_handle.abort();

        Ok(())
    }

    async fn wait_for_listen_addr(swarm: &mut libp2p::Swarm<Behaviour>) 
        -> Result<Multiaddr, Box<dyn Error>> 
    {
        timeout(Duration::from_secs(5), async {
            while let Some(event) = swarm.next().await {
                if let SwarmEvent::NewListenAddr { address, .. } = event {
                    return Ok(address);
                }
            }
            Err("No listen address received".into())
        })
        .await?
    }
    #[tokio::test]
    async fn test_mesh_network_discovery() -> Result<(), Box<dyn Error>> {

        init_logger();

        info!("Starting mesh network test");
        
        // Create nodes
        let (mut node1, mut events_rx1) = DHTNode::new().await?;
        let (mut node2, mut events_rx2) = DHTNode::new().await?;
        let (mut node3, mut events_rx3) = DHTNode::new().await?;
        let (mut node4, mut events_rx4) = DHTNode::new().await?;

        // Start bootstrap node (node1)
        let addr1 = "/ip4/127.0.0.1/tcp/0".parse()?;
        node1.swarm.listen_on(addr1)?;
        let listen_addr1 = wait_for_listen_addr(&mut node1.swarm).await?;
        info!("Bootstrap node listening on {}", listen_addr1);

        // Start nodes with delay between each
        let node1_handle = tokio::spawn(async move {
            if let Err(e) = node1.start(None).await {
                error!("Node1 error: {}", e);
            }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        
        // Connect node2
        node2.connect_to_peer(listen_addr1.clone()).await?;
        let node2_peers = node2.get_peers();
        let node2_handle = tokio::spawn(async move {
            if let Err(e) = node2.start(None).await {
                error!("Node2 error: {}", e);
            }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Connect node3
        node3.connect_to_peer(listen_addr1.clone()).await?;
        let node3_handle = tokio::spawn(async move {
            if let Err(e) = node3.start(None).await {
                error!("Node3 error: {}", e);
            }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Connect node4
        node4.connect_to_peer(listen_addr1.clone()).await?;
        let node4_handle = tokio::spawn(async move {
            if let Err(e) = node4.start(None).await {
                error!("Node4 error: {}", e);
            }
        });

        // Wait for peer discovery with increased timeout
        timeout(Duration::from_secs(60), async {
            let mut connected_peers = HashSet::new();
            
            while connected_peers.len() < 3 {  // Wait for at least 3 peer connections
                tokio::select! {
                    Ok(event) = events_rx1.recv() => {
                        if let DHTEvent::PeerConnected(peer_id) = event {
                            info!("Bootstrap node connected to: {}", peer_id);
                            connected_peers.insert(peer_id);
                        }
                    }
                }
            }
        })
        .await?;
        

        // Clean up
        node1_handle.abort();
        node2_handle.abort();
        node3_handle.abort();
        node4_handle.abort();

        Ok(())
    }
}