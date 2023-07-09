package com.trackerhub.orders;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;

public class OrderTaskFactory extends Thread {
    private final int minimumPackagesPerTask;

    private final BlockingQueue<OrderLine> ordersReceived;

    private final BlockingQueue<OrderTask> ordersScheduled;

    public OrderTaskFactory(int minimumPackagesPerTask,
                            BlockingQueue<OrderLine> ordersReceived,
                            BlockingQueue<OrderTask> ordersScheduled) {
        this.minimumPackagesPerTask = minimumPackagesPerTask;
        this.ordersReceived = ordersReceived;
        this.ordersScheduled = ordersScheduled;
    }

    @Override
    public void run() {
        // convert orders into smaller ones
        while (true) {
            try {
                var order = ordersReceived.take();
                var tasks = generateTaskList(order);
                for (var task: tasks) {
                    ordersScheduled.put(task);
                }
            } catch (InterruptedException ignore) {
                return;
            }
        }
    }

    private List<OrderTask> generateTaskList(OrderLine orderLine) {
        int packages = orderLine.getTotalPackages();
        var res = new ArrayList<OrderTask>();
        OrderTask task;
        while (packages > minimumPackagesPerTask) {
            task = new OrderTask(orderLine, minimumPackagesPerTask);
            res.add(task);
            packages -= minimumPackagesPerTask;
        }
        task = new OrderTask(orderLine, packages);
        res.add(task);
        return res;
    }
}
