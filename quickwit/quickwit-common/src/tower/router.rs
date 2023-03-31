use std::collections::HashMap;
use std::task::{Context, Poll};

use tower::Service;

pub type NodeId = String;

pub type NodeIdRef = str;

pub enum PacketAddress {
    /// Send to any node in the cluster.
    Anycast,
    /// Send to all the nodes in the cluster.
    Broadcast,
    /// Send to a specific node.
    Unicast(NodeId),
    /// Send to a list of nodes.
    Multicast(Vec<NodeId>),
}

pub struct Packet<R> {
    pub destination: PacketAddress,
    pub payload: R,
}

impl<R> Packet<R> {
    pub fn unicast(node_id: NodeId, payload: R) -> Self {
        Self {
            destination: PacketAddress::Unicast(node_id),
            payload,
        }
    }
}

impl<S> Default for Pool<S> {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
        }
    }
}

#[derive(Debug)]
struct Pool<T> {
    nodes: HashMap<NodeId, T>,
}

impl<T> Pool<T> {
    pub fn add_node(&mut self, node_id: &NodeIdRef, payload: T) {
        self.nodes.insert(node_id.to_owned(), payload);
    }

    pub fn remove_node(&mut self, node_id: &NodeIdRef) -> Option<T> {
        self.nodes.remove(node_id)
    }

    pub fn find_node(&self, node_id: &NodeIdRef) -> Option<&T> {
        self.nodes.get(node_id)
    }
}

impl<S, R> Service<R> for Pool<S>
where S: Service<R> + Clone
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // FIXME
    }

    fn call(&mut self, request: R) -> Self::Future {
    }
}

struct Router<S> {
    pool: Pool<S>,
}

impl<S, R> Service<Packet<R>> for Router<S>
where S: Service<R> + Clone
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // FIXME
    }

    fn call(&mut self, request: Packet<R>) -> Self::Future {
        match request.destination {
            PacketAddress::Unicast(node_id) => {
                let mut service = self.pool.find_node(&node_id).unwrap().clone();
                service.call(request.request)
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use futures::future::{ready, Ready};

    use super::*;

    struct HelloRequest {
        name: String,
    }

    struct HelloService {
        node_id: NodeId,
    }

    impl HelloService {
        pub fn new(node_id: &NodeIdRef) -> Self {
            Self {
                node_id: node_id.to_owned(),
            }
        }
    }

    impl Service<HelloRequest> for HelloService {
        type Response = String;
        type Error = Infallible;
        type Future = Ready<Result<String, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: HelloRequest) -> Self::Future {
            ready(Ok(format!(
                "Hello, {} from {}!",
                request.name, self.node_id
            )))
        }
    }

    #[tokio::test]
    async fn test_pool() {
        let mut pool = Pool::default();
        pool.add_node("node1", HelloService::new("node1"));
        pool.add_node("node2", HelloService::new("node2"));

        // let mut router

        // let packet = Packet::unicast("node1".to_owned(), HelloRequest { name: "John".to_owned()
        // }); pool.hello(packet).await.unwrap();
    }
}
