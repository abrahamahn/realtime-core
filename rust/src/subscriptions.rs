use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SubscriptionStats {
    pub streams: usize,
    pub subscriptions: usize,
    pub connections: usize,
}

#[derive(Clone, Debug, Default)]
pub struct SubscriptionRegistry<Stream, Connection> {
    by_stream: BTreeMap<Stream, BTreeSet<Connection>>,
    by_connection: BTreeMap<Connection, BTreeSet<Stream>>,
}

impl<Stream, Connection> SubscriptionRegistry<Stream, Connection>
where
    Stream: Clone + Ord,
    Connection: Clone + Ord,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_stream: BTreeMap::new(),
            by_connection: BTreeMap::new(),
        }
    }

    pub fn subscribe(&mut self, stream: Stream, connection: Connection) -> bool {
        let added = self
            .by_stream
            .entry(stream.clone())
            .or_default()
            .insert(connection.clone());
        self.by_connection
            .entry(connection)
            .or_default()
            .insert(stream);
        added
    }

    pub fn unsubscribe(&mut self, stream: &Stream, connection: &Connection) -> bool {
        let removed = self
            .by_stream
            .get_mut(stream)
            .is_some_and(|connections| connections.remove(connection));
        if self.by_stream.get(stream).is_some_and(BTreeSet::is_empty) {
            self.by_stream.remove(stream);
        }
        if let Some(streams) = self.by_connection.get_mut(connection) {
            streams.remove(stream);
            if streams.is_empty() {
                self.by_connection.remove(connection);
            }
        }
        removed
    }

    pub fn remove_connection(&mut self, connection: &Connection) -> usize {
        let Some(streams) = self.by_connection.remove(connection) else {
            return 0;
        };
        let count = streams.len();
        for stream in streams {
            if let Some(connections) = self.by_stream.get_mut(&stream) {
                connections.remove(connection);
                if connections.is_empty() {
                    self.by_stream.remove(&stream);
                }
            }
        }
        count
    }

    pub fn subscribers(&self, stream: &Stream) -> Vec<&Connection> {
        self.by_stream
            .get(stream)
            .map_or_else(Vec::new, |connections| connections.iter().collect())
    }

    #[must_use]
    pub fn subscriber_count(&self, stream: &Stream) -> usize {
        self.by_stream.get(stream).map_or(0, BTreeSet::len)
    }

    #[must_use]
    pub fn stream_count(&self) -> usize {
        self.by_stream.len()
    }

    #[must_use]
    pub fn stats(&self) -> SubscriptionStats {
        SubscriptionStats {
            streams: self.by_stream.len(),
            subscriptions: self.by_stream.values().map(BTreeSet::len).sum(),
            connections: self.by_connection.len(),
        }
    }
}
