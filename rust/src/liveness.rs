use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LivenessSweep<Connection> {
    pub probe: Vec<Connection>,
    pub stale: Vec<Connection>,
}

/// Pure acknowledgement state for heartbeat adapters.
#[derive(Clone, Debug, Default)]
pub struct LivenessTracker<Connection> {
    alive: BTreeMap<Connection, bool>,
}

impl<Connection> LivenessTracker<Connection>
where
    Connection: Clone + Ord,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            alive: BTreeMap::new(),
        }
    }

    pub fn track(&mut self, connection: Connection) -> bool {
        if self.alive.contains_key(&connection) {
            return false;
        }
        self.alive.insert(connection, true);
        true
    }

    pub fn acknowledge(&mut self, connection: &Connection) -> bool {
        let Some(alive) = self.alive.get_mut(connection) else {
            return false;
        };
        *alive = true;
        true
    }

    pub fn remove(&mut self, connection: &Connection) -> bool {
        self.alive.remove(connection).is_some()
    }

    pub fn sweep(&mut self) -> LivenessSweep<Connection> {
        let mut probe = Vec::new();
        let mut stale = Vec::new();
        for (connection, alive) in &mut self.alive {
            if *alive {
                probe.push(connection.clone());
                *alive = false;
            } else {
                stale.push(connection.clone());
            }
        }
        for connection in &stale {
            self.alive.remove(connection);
        }
        LivenessSweep { probe, stale }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.alive.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.alive.is_empty()
    }

    pub fn clear(&mut self) -> usize {
        let removed = self.alive.len();
        self.alive.clear();
        removed
    }
}
