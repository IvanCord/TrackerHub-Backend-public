package com.trackerhub.orders;

import com.trackerhub.backend.dtos.NewDailyOrderDTO;
import com.trackerhub.backend.services.OrderScheduler;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.awt.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.StringTokenizer;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
public class OrderLine implements Comparable<OrderLine> {
    private UUID orderId = UUID.randomUUID();
    private long clientId;
    private Point destination;
    private LocalTime timeArrival;
    private LocalDateTime dateArrival;
    private int totalPackages;
    private int hoursLimit; //hours
    private LocalDateTime dateLimit;

    public OrderLine(LocalDateTime date, String orderString) {
        var tokenizer = new StringTokenizer(orderString, ",");
        var times = tokenizer.nextToken().split(":");
        this.timeArrival = LocalTime.of(Integer.parseInt(times[0]), Integer.parseInt(times[1]));
        this.dateArrival = LocalDateTime.of(date.toLocalDate(), timeArrival);
        this.destination = new Point(Integer.parseInt(tokenizer.nextToken()), Integer.parseInt(tokenizer.nextToken()));
        this.totalPackages = Integer.parseInt(tokenizer.nextToken());
        this.clientId = Integer.parseInt(tokenizer.nextToken());
        this.hoursLimit = Integer.parseInt(tokenizer.nextToken());
        this.dateLimit = LocalDateTime.of(date.toLocalDate(), timeArrival).plusHours(hoursLimit);
    }

    public OrderLine(NewDailyOrderDTO orderDTO) {
        var date = OrderScheduler.timer;
        this.timeArrival = date.toLocalTime();
        this.destination = new Point(orderDTO.getPosX(), orderDTO.getPosY());
        this.totalPackages = orderDTO.getNumPaq();
        this.clientId = 1000;
        this.hoursLimit = orderDTO.getHoursLimit();
        this.dateLimit = LocalDateTime.of(date.toLocalDate(), timeArrival).plusHours(hoursLimit);
    }

    @Override
    public int compareTo(OrderLine line) {
        return dateLimit.compareTo(line.dateLimit);
    }
}
