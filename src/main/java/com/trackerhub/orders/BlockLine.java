package com.trackerhub.orders;

import lombok.Getter;
import lombok.Setter;

import java.awt.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

@Getter
@Setter
public class BlockLine {
    private LocalDateTime from;

    private LocalDateTime until;

    private List<Point> coordinates;

    public BlockLine(String blockLine, LocalDate date) {
        var tokenizer = new StringTokenizer(blockLine, ",");
        var interval = tokenizer.nextToken().split("-");
        var fromArr = interval[0].split(":");
        var fromDate = date.withDayOfMonth(Integer.parseInt(fromArr[0]));
        var fromTime = LocalTime.of(Integer.parseInt(fromArr[1]), Integer.parseInt(fromArr[2]));
        this.from = LocalDateTime.of(fromDate, fromTime);
        var untilArr = interval[1].split(":");
        var untilDate = date.withDayOfMonth(Integer.parseInt(untilArr[0]));
        var untilTime = LocalTime.of(Integer.parseInt(untilArr[1]), Integer.parseInt(untilArr[2]));
        this.until = LocalDateTime.of(untilDate, untilTime);
        coordinates = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            var x = Integer.parseInt(tokenizer.nextToken());
            var y = Integer.parseInt(tokenizer.nextToken());
            var pair = new Point(x, y);
            coordinates.add(pair);
        }
    }
}
