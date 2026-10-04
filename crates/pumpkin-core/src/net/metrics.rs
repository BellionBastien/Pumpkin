use std::fmt::{Display, Write as _};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::{select, time};
use tracing::{debug, error, info};

use crate::server::tick_histogram::TickDurationHistogram;
use crate::world::World;
use crate::{STOP_INTERRUPT, server::Server};

const METRICS_PATH: &str = "/metrics";
/// Header for the Prometheus text exposition format.
const METRICS_CONTENT_TYPE: &str = "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n";
/// Upper bound for the request line and headers. A scraper sends well under a kilobyte.
const MAX_REQUEST_HEAD_BYTES: u64 = 8 * 1024;
/// Time allowed to read the request and write the response.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
/// Connections served at once. Further ones are closed right away.
const MAX_CONNECTIONS: usize = 8;
/// Pause after a failed accept, so a persistent error such as running out of file
/// descriptors doesn't spin.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(500);
const NANOSECONDS_PER_SECOND: f64 = 1_000_000_000.0;

/// Serves the Prometheus metrics endpoint until the server stops.
pub async fn start_metrics_handler(server: Arc<Server>, address: SocketAddr) {
    let listener = match TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(err) => {
            error!("Failed to bind the metrics endpoint on {address}: {err}");
            return;
        }
    };
    info!("Metrics endpoint is listening on {address}");

    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let accepted = select! {
            accepted = listener.accept() => accepted,
            () = STOP_INTERRUPT.cancelled() => break,
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(err) => {
                debug!("Metrics endpoint failed to accept a connection: {err}");
                time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            debug!("Metrics endpoint is busy, closing a connection");
            continue;
        };

        let server = server.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_connection(stream, &server).await;
        });
    }
}

async fn handle_connection(mut stream: TcpStream, server: &Server) {
    let exchange = async {
        let request_line = read_request_line(&mut stream).await?;
        stream
            .write_all(respond(&request_line, server).as_bytes())
            .await?;
        stream.shutdown().await
    };
    if let Ok(Err(err)) = time::timeout(CONNECTION_TIMEOUT, exchange).await {
        debug!("Metrics connection failed: {err}");
    }
}

/// Reads the request line and discards the headers after it.
async fn read_request_line(stream: &mut TcpStream) -> io::Result<String> {
    let mut reader = BufReader::new(stream.take(MAX_REQUEST_HEAD_BYTES));
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    // Closing a socket that still has unread data resets the connection, which can cost the
    // client the response.
    let mut header = String::new();
    loop {
        header.clear();
        if reader.read_line(&mut header).await? == 0 || header.trim().is_empty() {
            return Ok(request_line);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Route {
    Metrics,
    BadRequest,
    NotFound,
    MethodNotAllowed,
}

fn route(request_line: &str) -> Route {
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Route::BadRequest;
    };
    if !version.starts_with("HTTP/") {
        return Route::BadRequest;
    }

    let path = target.split_once('?').map_or(target, |(path, _query)| path);
    if path != METRICS_PATH {
        Route::NotFound
    } else if method != "GET" {
        Route::MethodNotAllowed
    } else {
        Route::Metrics
    }
}

fn respond(request_line: &str, server: &Server) -> String {
    match route(request_line) {
        Route::Metrics => http_response("200 OK", METRICS_CONTENT_TYPE, &render_metrics(server)),
        Route::BadRequest => http_response("400 Bad Request", "", ""),
        Route::NotFound => http_response("404 Not Found", "", ""),
        Route::MethodNotAllowed => http_response("405 Method Not Allowed", "Allow: GET\r\n", ""),
    }
}

fn http_response(status: &str, headers: &str, body: &str) -> String {
    let length = body.len();
    format!(
        "HTTP/1.1 {status}\r\n{headers}Content-Length: {length}\r\nConnection: close\r\n\r\n{body}"
    )
}

fn render_metrics(server: &Server) -> String {
    let mut out = String::new();
    write_tick_duration_histogram(&mut out, &server.tick_duration_histogram);

    // Same value as `/tps`: the average over the last 100 ticks, capped at the tick rate.
    let tps = server.get_tps().min(f64::from(server.basic_config.tps));
    write_gauge(
        &mut out,
        "pumpkin_tps",
        "Ticks per second over the last 100 ticks, capped at the configured tick rate.",
        tps,
    );
    write_gauge(
        &mut out,
        "pumpkin_players_online",
        "Players currently connected.",
        server.get_player_count(),
    );
    write_gauge(
        &mut out,
        "pumpkin_players_max",
        "Maximum number of players allowed.",
        server.max_players(),
    );

    let worlds = server.worlds.load();
    write_header(
        &mut out,
        "pumpkin_world_loaded_chunks",
        "gauge",
        "Chunks loaded in memory.",
    );
    for world in worlds.iter() {
        write_world_sample(
            &mut out,
            "pumpkin_world_loaded_chunks",
            world,
            world.level.loaded_chunk_count(),
        );
    }
    write_header(
        &mut out,
        "pumpkin_world_entities",
        "gauge",
        "Entities in the world, players excluded.",
    );
    for world in worlds.iter() {
        write_world_sample(
            &mut out,
            "pumpkin_world_entities",
            world,
            world.entities.load().len(),
        );
    }
    out
}

fn write_tick_duration_histogram(out: &mut String, histogram: &TickDurationHistogram) {
    const NAME: &str = "pumpkin_tick_duration_seconds";
    write_header(
        out,
        NAME,
        "histogram",
        "Time spent processing a server tick, without the wait for the next one.",
    );

    let mut count = 0;
    for (upper_bound_nanos, cumulative) in histogram.cumulative_buckets() {
        let le = upper_bound_nanos.map_or_else(
            || "+Inf".to_owned(),
            |nanos| nanos_to_seconds(nanos).to_string(),
        );
        let _ = writeln!(out, "{NAME}_bucket{{le=\"{le}\"}} {cumulative}");
        count = cumulative;
    }
    let _ = writeln!(
        out,
        "{NAME}_sum {}",
        nanos_to_seconds(histogram.sum_nanos())
    );
    let _ = writeln!(out, "{NAME}_count {count}");
}

fn write_header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn write_gauge(out: &mut String, name: &str, help: &str, value: impl Display) {
    write_header(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {value}");
}

fn write_world_sample(out: &mut String, name: &str, world: &World, value: usize) {
    let _ = writeln!(
        out,
        "{name}{{world=\"{}\",dimension=\"{}\"}} {value}",
        escape_label_value(world.get_world_name()),
        escape_label_value(world.dimension.minecraft_name),
    );
}

/// Escapes backslashes, double quotes and line feeds, as the text format requires in label
/// values. Plugins choose the world names.
fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn nanos_to_seconds(nanos: u64) -> f64 {
    nanos as f64 / NANOSECONDS_PER_SECOND
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_duration_histogram_text_format() {
        let histogram = TickDurationHistogram::default();
        // 1 ms and 50 ms sit exactly on a bound and belong to that bucket.
        for nanos in [500_000, 1_000_000, 50_000_000, 60_000_000, 2_000_000_000] {
            histogram.observe(nanos);
        }

        let mut out = String::new();
        write_tick_duration_histogram(&mut out, &histogram);

        let lines: Vec<&str> = out
            .lines()
            .filter(|line| !line.starts_with("# HELP"))
            .collect();
        assert_eq!(
            lines,
            [
                "# TYPE pumpkin_tick_duration_seconds histogram",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.001\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.0025\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.005\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.01\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.02\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.03\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.04\"} 2",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.05\"} 3",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.075\"} 4",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.1\"} 4",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.25\"} 4",
                "pumpkin_tick_duration_seconds_bucket{le=\"0.5\"} 4",
                "pumpkin_tick_duration_seconds_bucket{le=\"1\"} 4",
                "pumpkin_tick_duration_seconds_bucket{le=\"+Inf\"} 5",
                "pumpkin_tick_duration_seconds_sum 2.1115",
                "pumpkin_tick_duration_seconds_count 5",
            ]
        );
    }

    #[test]
    fn label_values_are_escaped() {
        assert_eq!(escape_label_value("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn requests_are_routed() {
        assert_eq!(route("GET /metrics HTTP/1.1\r\n"), Route::Metrics);
        assert_eq!(route("GET /metrics?debug=1 HTTP/1.1\r\n"), Route::Metrics);
        assert_eq!(route("GET / HTTP/1.1\r\n"), Route::NotFound);
        assert_eq!(route("POST /metrics HTTP/1.1\r\n"), Route::MethodNotAllowed);
        assert_eq!(route("GET /metrics\r\n"), Route::BadRequest);
        assert_eq!(route("\x16\x03\x01"), Route::BadRequest);
        assert_eq!(route(""), Route::BadRequest);
    }
}
