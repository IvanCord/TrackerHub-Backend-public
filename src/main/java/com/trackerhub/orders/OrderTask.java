package com.trackerhub.orders;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Getter
@NoArgsConstructor
public class OrderTask implements Comparable<OrderTask> {
    private final UUID taskId = UUID.randomUUID();

    protected OrderLine order;

    protected int numPackages;

    public OrderTask(OrderLine order, int numPackages) {
        this.order = order;
        this.numPackages = numPackages;
    }

    public boolean isRescue() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderTask orderTask)) return false;

        return taskId.equals(orderTask.taskId);
    }

    @Override
    public int hashCode() {
        return taskId.hashCode();
    }

    @Override
    public int compareTo(OrderTask task) {
        return order.getDateLimit().compareTo(task.getOrder().getDateLimit());
    }
}
