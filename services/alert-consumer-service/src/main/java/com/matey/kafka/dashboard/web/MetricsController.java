package com.matey.kafka.dashboard.web;

import com.matey.kafka.dashboard.metrics.MetricsAggregator;
import com.matey.kafka.dashboard.metrics.MetricsAggregator.DashboardMetricsSnapshot;
import com.matey.kafka.dashboard.metrics.MetricsAggregator.RuleLatencySummary;
import com.matey.kafka.dashboard.metrics.MetricsAggregator.PercentilesSummary;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * REST API for the dashboard frontend.
 *
 * <p>The single-page UI ({@code src/main/resources/static/index.html}) polls a single
 * combined endpoint ({@code GET /metrics/dashboard}) for alert-side metrics (counts,
 * alerts/sec, latency distribution, per-rule breakdowns). A reset endpoint is provided
 * so experiments can be started from a clean slate ({@code POST /metrics/reset}).</p>
 */
@RestController
@RequestMapping("/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final MetricsAggregator metricsAggregator;

    @GetMapping("/dashboard")
    public DashboardMetricsResponse getDashboardMetrics() {
        DashboardMetricsSnapshot snap = metricsAggregator.snapshot();

        DashboardMetricsResponse resp = new DashboardMetricsResponse();

        DashboardMetricsResponse.Summary summary = new DashboardMetricsResponse.Summary();
        summary.setTotalAlerts(snap.getTotalAlerts());

        double alertsPerSecond = snap.getUptimeSeconds() > 0
                ? snap.getTotalAlerts() / snap.getUptimeSeconds()
                : 0.0;
        summary.setAlertsPerSecond(alertsPerSecond);

        summary.setLatencyAvgMillis(snap.getLatencyAvgMs());
        summary.setLatencyMaxMillis(snap.getLatencyMaxMs());
        resp.setSummary(summary);

        resp.setAlertsByType(snap.getAlertsPerType());

        Map<String, DashboardMetricsResponse.LatencyByType> latencyByType = new HashMap<>();
        Map<String, RuleLatencySummary> perRule = snap.getPerRuleLatency();
        if (perRule != null) {
            perRule.forEach((rule, stats) -> {
                DashboardMetricsResponse.LatencyByType dto =
                        new DashboardMetricsResponse.LatencyByType();
                dto.setCount(stats.getCount());
                dto.setAvgMillis(stats.getAvgMs());
                dto.setMinMillis(stats.getMinMs());
                dto.setMaxMillis(stats.getMaxMs());
                latencyByType.put(rule, dto);
            });
        }
        resp.setLatencyByType(latencyByType);

        PercentilesSummary p = snap.getLatencyPercentiles();
        DashboardMetricsResponse.LatencyPercentiles lp =
                new DashboardMetricsResponse.LatencyPercentiles();
        if (p != null) {
            lp.setCount(p.getCount());
            lp.setP50(p.getP50());
            lp.setP75(p.getP75());
            lp.setP90(p.getP90());
            lp.setP95(p.getP95());
            lp.setP99(p.getP99());
        }
        resp.setLatencyPercentiles(lp);

        // ---- throughput series for the chart (alerts/second over time) ----
        // index.html uses this if present; otherwise it falls back to a local history buffer.
        List<DashboardMetricsResponse.ThroughputPoint> series =
                metricsAggregator.getThroughputSeries(300)
                        .stream()
                        .map(pnt -> {
                            DashboardMetricsResponse.ThroughputPoint dto =
                                    new DashboardMetricsResponse.ThroughputPoint();
                            dto.setEpochSecond(pnt.getEpochSecond());
                            dto.setAlerts(pnt.getAlerts());
                            return dto;
                        })
                        .toList();
        resp.setThroughputSeries(series);

        return resp;
    }

    /**
     * Reset all alert-side metrics.
     * POST /metrics/reset
     */
    @PostMapping("/reset")
    public void resetMetrics() {
        metricsAggregator.startNewRun(0.0);
    }
}
