package com.trackerhub.orders;

import com.trackerhub.scheduling.VehicleState;
import com.trackerhub.scheduling.VehicleStateChange;
import com.trackerhub.scheduling.VehicleThread;
import com.trackerhub.scheduling.VehicleType;

import java.util.*;
import java.util.concurrent.BlockingQueue;


public class OrderTaskScheduler extends Thread {
    private final Map<VehicleType, Map<VehicleState, Map<String, VehicleThread>>> vehicleThreads;
    private final BlockingQueue<OrderTask> ordersScheduled;
    private final BlockingQueue<VehicleStateChange> vehicleStateChanges;

    public OrderTaskScheduler(Map<VehicleType, Map<VehicleState, Map<String, VehicleThread>>> vehicleThreads,
                              BlockingQueue<OrderTask> ordersScheduled,
                              BlockingQueue<VehicleStateChange> vehicleStateChanges) {
        this.vehicleThreads = vehicleThreads;
        this.ordersScheduled = ordersScheduled;
        this.vehicleStateChanges = vehicleStateChanges;
    }

    @Override
    public void run() {
        while (true) {
            try {
                var order = ordersScheduled.take();
                var stateChanges = new HashMap<VehicleThread, Queue<OrderTask>>();
                var availableCars = Map.copyOf(vehicleThreads.get(VehicleType.CAR).get(VehicleState.AVAILABLE));
                var availableBikes = Map.copyOf(vehicleThreads.get(VehicleType.MOTORCYCLE).get(VehicleState.AVAILABLE));

                if (availableCars.isEmpty() && availableBikes.isEmpty()) {
                    ordersScheduled.put(order);
//                        System.out.println("No vehicles left to schedule, remaining orders: " + ordersScheduled.size());
                    Thread.sleep(100);
                    continue;
                }

                // extract all orders for processing
                var ordersToSchedule = new PriorityQueue<OrderTask>();
                ordersToSchedule.add(order);
                ordersScheduled.drainTo(ordersToSchedule);

                // car scheduling
                if (!availableCars.isEmpty()) {
                    var iter = availableCars.entrySet().iterator();
                    while (true) {
                        Map.Entry<String, VehicleThread> carThread;
                        try {
                            carThread = iter.next();
                        } catch (NoSuchElementException ex) {
                            break;
                        }
                        var carOrders = new PriorityQueue<OrderTask>();
                        int sum = fillOrderQueue(ordersToSchedule, carOrders, 40);
                        if (sum == 0) {
                            break;
                        }
                        if (sum >= 16 || availableBikes.isEmpty()) {
//                                System.out.println("Scheduling car with " + sum + " packages and " + carOrders.size() + " tasks.");
                            stateChanges.put(carThread.getValue(), carOrders);
                        } else {
                            // return orders for bike usage
                            ordersToSchedule.addAll(carOrders);
                            break;
                        }
                    }
                }

                // bike scheduling
                if (!availableBikes.isEmpty()) {
                    var iter = availableBikes.entrySet().iterator();
                    while (true) {
                        Map.Entry<String, VehicleThread> bikeThread;
                        try {
                            bikeThread = iter.next();
                        } catch (NoSuchElementException ex) {
                            break;
                        }
                        var bikeOrders = new PriorityQueue<OrderTask>();
                        int sum = fillOrderQueue(ordersToSchedule, bikeOrders, 8);
                        if (sum == 0) break;
//                            System.out.println("Scheduling motorcycle with " + sum + " packages and " + bikeOrders.size() + " tasks.");
                        stateChanges.put(bikeThread.getValue(), bikeOrders);
                    }
                }

                // apply state changes
                for (var entry: stateChanges.entrySet()) {
                    var thread = entry.getKey();
                    var task = entry.getValue();
                    vehicleStateChanges.put(new VehicleStateChange(VehicleState.AVAILABLE, VehicleState.BUSY, thread.getVehicleType(), thread.getVehicleId()));
                    thread.assignTask(task);
                }

                // return any remaining orders
                for (var remaining: ordersToSchedule) {
                    ordersScheduled.put(remaining);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private int fillOrderQueue(Queue<OrderTask> toSchedule, Queue<OrderTask> scheduled, int capacity) {
        int totalSum = 0;
        while (true) {
            var task = toSchedule.poll();
            if (task == null) break;
            totalSum += task.getNumPackages();
            if (totalSum > capacity) {
                totalSum -= task.getNumPackages();
                toSchedule.add(task);
                return totalSum;
            } else {
                scheduled.add(task);
            }
        }
        return totalSum;
    }
}
