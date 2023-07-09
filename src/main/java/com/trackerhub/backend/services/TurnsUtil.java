package com.trackerhub.backend.services;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import static com.trackerhub.backend.services.OrderScheduler.timer;

public class TurnsUtil {
    private final LocalTime t12 = LocalTime.of(8, 0);
    private final LocalTime t23 = LocalTime.of(16, 0);
    private final LocalTime tend = LocalTime.of(23, 59);
    private final LocalTime tbeg = LocalTime.of(0, 0);

    public long getTimeUntilNextTurn() {
        int turn = getCurrentTurn();
        var time = timer.toLocalTime();
        if (turn == 1) {
            return ChronoUnit.MILLIS.between(time, t12);
        }
        if (turn == 2) {
            return ChronoUnit.MILLIS.between(time, t23);
        }
        if (turn == 3) {
            return ChronoUnit.MILLIS.between(time, tend);
        }
        return 0;
    }

    public long getDelayDuration(int breakdownType) {
        if (breakdownType == 1) {
            return getMillisBetween(2);
        }
        if (breakdownType == 2) {
            return getTurnDurationMillis() + getTimeUntilNextTurn();
        }
        if (breakdownType == 3) {
            var next = LocalDateTime.of(timer.toLocalDate().plusDays(3), t12);
            return ChronoUnit.MILLIS.between(timer, next);
        }
        return 0;
    }

    public long getTurnDurationMillis() {
        return getMillisBetween(8);
    }

    public long getMillisBetween(int hour) {
        var end = LocalTime.of(hour, 0);
        return ChronoUnit.MILLIS.between(tbeg, end);
    }

    public int getCurrentTurn() {
        var time = timer.toLocalTime();
        if (time.isBefore(t12)) {
            return 1;
        }
        if (time.isBefore(t23)) {
            return 2;
        }
        if (time.isBefore(tend)) {
            return 3;
        }
        return 0;
    }
}
