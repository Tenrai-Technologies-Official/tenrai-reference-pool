use super::Api;
use crate::{config::Mode, db::Writer, stratum::protocol::Protocol};
use axum::{
    extract::State,
    http::header,
    response::{IntoResponse, Response},
};
use std::fmt::Write;

/// Prometheus text format.
pub async fn serve(State(api): State<Api>) -> Response {
    let hub = &api.hub;
    let mut out = String::with_capacity(4_096);
    let network = hub.stats.network();
    gauge(&mut out, "tenrai_pool_up", "1 while the pool runs", 1.0);
    gauge(
        &mut out,
        "tenrai_pool_uptime_seconds",
        "Seconds since the pool started",
        hub.stats.uptime().as_secs_f64(),
    );
    let protocols: Vec<_> = Protocol::ALL
        .into_iter()
        .filter(|protocol| {
            hub.config
                .listeners
                .iter()
                .any(|listener| listener.protocol == *protocol)
        })
        .map(|protocol| (protocol.name(), hub.stats.protocol(protocol)))
        .collect();
    family(
        &mut out,
        "tenrai_pool_connections",
        "gauge",
        "Open stratum connections by protocol",
    );
    for (protocol, totals) in &protocols {
        let _ = writeln!(
            out,
            "tenrai_pool_connections{{protocol=\"{protocol}\"}} {}",
            totals.connections
        );
    }
    family(
        &mut out,
        "tenrai_pool_protocol_shares_total",
        "counter",
        "Shares by protocol and outcome",
    );
    for (protocol, totals) in &protocols {
        for (result, value) in [
            ("accepted", totals.accepted),
            ("rejected", totals.rejected),
            ("stale", totals.stale),
        ] {
            let _ = writeln!(
                out,
                "tenrai_pool_protocol_shares_total{{protocol=\"{protocol}\",result=\"{result}\"}} {value}"
            );
        }
    }
    let modes: Vec<(Mode, _)> = Mode::ALL
        .iter()
        .map(|mode| (*mode, hub.stats.mode(*mode)))
        .collect();
    family(
        &mut out,
        "tenrai_pool_workers",
        "gauge",
        "Connected workers",
    );
    for (mode, view) in &modes {
        sample(&mut out, "tenrai_pool_workers", mode, view.workers as f64);
    }
    family(
        &mut out,
        "tenrai_pool_miners",
        "gauge",
        "Connected payout addresses",
    );
    for (mode, view) in &modes {
        sample(&mut out, "tenrai_pool_miners", mode, view.miners as f64);
    }
    family(
        &mut out,
        "tenrai_pool_hashrate",
        "gauge",
        "Estimated hashes per second over five minutes",
    );
    for (mode, view) in &modes {
        sample(&mut out, "tenrai_pool_hashrate", mode, view.hashrate);
    }
    family(
        &mut out,
        "tenrai_pool_shares_accepted_total",
        "counter",
        "Accepted shares",
    );
    for (mode, view) in &modes {
        sample(
            &mut out,
            "tenrai_pool_shares_accepted_total",
            mode,
            view.accepted as f64,
        );
    }
    family(
        &mut out,
        "tenrai_pool_shares_rejected_total",
        "counter",
        "Rejected shares",
    );
    for (mode, view) in &modes {
        sample(
            &mut out,
            "tenrai_pool_shares_rejected_total",
            mode,
            view.rejected as f64,
        );
    }
    family(
        &mut out,
        "tenrai_pool_shares_stale_total",
        "counter",
        "Shares for stale jobs",
    );
    for (mode, view) in &modes {
        sample(
            &mut out,
            "tenrai_pool_shares_stale_total",
            mode,
            view.stale as f64,
        );
    }
    family(
        &mut out,
        "tenrai_pool_share_rejects_total",
        "counter",
        "Rejected and stale shares by reason",
    );
    for (reason, count) in hub.stats.rejects() {
        let _ = writeln!(
            out,
            "tenrai_pool_share_rejects_total{{reason=\"{reason}\"}} {count}"
        );
    }
    family(
        &mut out,
        "tenrai_pool_round_work",
        "gauge",
        "Share work since the last block",
    );
    for (mode, view) in &modes {
        sample(
            &mut out,
            "tenrai_pool_round_work",
            mode,
            view.round_work as f64,
        );
    }
    for (name, help, pick) in [
        ("tenrai_pool_blocks_found_total", "Blocks found", 0),
        (
            "tenrai_pool_blocks_open",
            "Blocks waiting for their reward",
            1,
        ),
        (
            "tenrai_pool_blocks_confirmed_total",
            "Blocks paid by the node",
            2,
        ),
        (
            "tenrai_pool_blocks_orphaned_total",
            "Blocks expired or rejected",
            3,
        ),
    ] {
        let kind = if pick == 1 { "gauge" } else { "counter" };
        family(&mut out, name, kind, help);
        for (mode, view) in &modes {
            let blocks = view.blocks;
            let value = [blocks.found, blocks.open, blocks.confirmed, blocks.lost][pick];
            sample(&mut out, name, mode, value as f64);
        }
    }
    gauge(
        &mut out,
        "tenrai_pool_node_connected",
        "1 while the node is synchronized and answers",
        f64::from(u8::from(network.synced)),
    );
    gauge(
        &mut out,
        "tenrai_pool_node_height",
        "Finalized cut height",
        network.height as f64,
    );
    gauge(
        &mut out,
        "tenrai_pool_network_difficulty",
        "Network difficulty of the newest work",
        network.difficulty as f64,
    );
    gauge(
        &mut out,
        "tenrai_pool_network_hashrate",
        "Network hashes per second reported by the node",
        network.hashrate,
    );
    let job_age = network.work_at.map_or(0.0, |at| {
        crate::db::now_millis().saturating_sub(at) as f64 / 1_000.0
    });
    gauge(
        &mut out,
        "tenrai_pool_job_age_seconds",
        "Age of the newest template",
        job_age,
    );
    let open = api
        .reader
        .try_read(|connection| {
            connection.query_row("SELECT COUNT(*) FROM blocks WHERE status<2", [], |row| {
                row.get::<_, u64>(0)
            })
        })
        .await;
    gauge(
        &mut out,
        "tenrai_pool_blocks_queued",
        "Blocks the node has not paid or refused yet",
        open.map(|open| open as f64).unwrap_or(f64::NAN),
    );
    gauge(
        &mut out,
        "tenrai_pool_pplns_log_shares",
        "Shares held for PPLNS windows",
        hub.pplns.lock().map_or(0, |log| log.len()) as f64,
    );
    gauge(
        &mut out,
        "tenrai_pool_db_queue_size",
        "Writes waiting for the database",
        hub.writer.queued() as f64,
    );
    gauge(
        &mut out,
        "tenrai_pool_acceptances_in_flight",
        "Proof checks or accepted writes still owned by physical work",
        hub.writer.pending_work() as f64,
    );
    gauge(
        &mut out,
        "tenrai_pool_receipt_replays_in_flight",
        "Bounded writer-fenced receipt reads",
        hub.writer.replaying_work() as f64,
    );
    counter(
        &mut out,
        "tenrai_pool_db_dropped_total",
        "Writes dropped because the database fell behind",
        Writer::dropped() as f64,
    );
    counter(
        &mut out,
        "tenrai_pool_connections_refused_total",
        "Connections refused by limits or bans",
        hub.stats.refusals() as f64,
    );
    gauge(
        &mut out,
        "tenrai_pool_banned",
        "Banned addresses",
        hub.admission.banned() as f64,
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
        .into_response()
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, mode: &Mode, value: f64) {
    // Adding 0.0 prints a -0.0 rate as 0.
    let _ = writeln!(out, "{name}{{mode=\"{}\"}} {}", mode.name(), value + 0.0);
}

fn gauge(out: &mut String, name: &str, help: &str, value: f64) {
    family(out, name, "gauge", help);
    let _ = writeln!(out, "{name} {}", value + 0.0);
}

fn counter(out: &mut String, name: &str, help: &str, value: f64) {
    family(out, name, "counter", help);
    let _ = writeln!(out, "{name} {}", value + 0.0);
}
