use std::net::{Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};

/// Configuration for the Prometheus metrics endpoint.
///
/// Controls whether the endpoint is enabled and which address it binds to.
#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Whether the metrics endpoint is enabled.
    pub enabled: bool,
    /// The address and port the metrics endpoint binds to.
    pub address: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            address: SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9225),
        }
    }
}
