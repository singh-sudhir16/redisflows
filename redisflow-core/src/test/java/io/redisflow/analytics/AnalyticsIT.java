package io.redisflow.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.redisflow.analytics.WorkerGeoIndex.WorkerLocation;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class AnalyticsIT extends CoreIntegrationTest {

    @Autowired
    VisitorAnalytics visitors;
    @Autowired
    ActivityTracker activity;
    @Autowired
    WorkerGeoIndex geo;

    @Test
    void hyperLogLogEstimatesUniqueVisitorsWithinTwoPercent() {
        for (int i = 0; i < 20_000; i++) {
            visitors.track("user-" + (i % 10_000), "10.0." + (i % 50) + ".1");
        }
        LocalDate today = ActivityTracker.today();

        assertThat((double) visitors.uniqueVisitors(today, today)).isCloseTo(10_000, within(200.0));
        assertThat(visitors.uniqueIps(today, today)).isEqualTo(50);
    }

    @Test
    void bitmapsTrackDailyActiveUsers() {
        activity.markActive(1);
        activity.markActive(42);
        activity.markActive(42);
        activity.markActive(1_000_000);
        LocalDate today = ActivityTracker.today();

        assertThat(activity.activeCount(today)).isEqualTo(3);
        assertThat(activity.isActive(42, today)).isTrue();
        assertThat(activity.isActive(43, today)).isFalse();
        assertThat(activity.activeOnAllDays(List.of(today, today.minusDays(1)))).isZero();
        assertThat(activity.activeOnAllDays(List.of(today))).isEqualTo(3);
    }

    @Test
    void geoSearchReturnsNearestWorkersFirst() {
        geo.register("bengaluru", 12.9716, 77.5946);
        geo.register("mysuru", 12.2958, 76.6394);
        geo.register("lima", -12.0464, -77.0428);

        List<WorkerLocation> nearby = geo.nearby(12.97, 77.59, 200, "km");

        assertThat(nearby).extracting(WorkerLocation::workerId).containsExactly("bengaluru", "mysuru");
        assertThat(geo.all()).hasSize(3);
    }
}
