package com.trackerhub.scheduling;

import com.trackerhub.backend.services.OrderScheduler;
import com.trackerhub.orders.OrderTask;
import com.trackerhub.orders.RescueTask;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class VehicleStateHandler extends Thread {
    private final BlockingQueue<VehicleStateChange> vehicleStateChanges;

    private final BlockingQueue<OrderTask> ordersScheduled;

    private final Map<VehicleType, Map<VehicleState, Map<String, VehicleThread>>> vehicleThreads;

    public VehicleStateHandler(BlockingQueue<VehicleStateChange> vehicleStateChanges,
                               Map<VehicleType, Map<VehicleState, Map<String, VehicleThread>>> vehicleThreads,
                               BlockingQueue<OrderTask> ordersScheduled) {
        this.vehicleStateChanges = vehicleStateChanges;
        this.vehicleThreads = vehicleThreads;
        this.ordersScheduled = ordersScheduled;
    }

    @Override
    public void run() {
        while (true) {
            VehicleStateChange stateChange;
            try {
                stateChange = vehicleStateChanges.take();
            } catch (InterruptedException e) {
                return;
            }
            if (stateChange.getNewState() == null && stateChange.getOldState() == null) {
                OrderScheduler.collapsed.set(true);
                return;
            }
            synchronized (vehicleThreads) {
                var vehicleType = vehicleThreads.get(stateChange.getType());
                var oldState = vehicleType.get(stateChange.getOldState());
                var newState = vehicleType.get(stateChange.getNewState());
                var thread = oldState.remove(stateChange.getThreadId());
                if (thread == null) {
                    continue;
                }
                if (stateChange.getNewState() == VehicleState.DAMAGED) {
                    var rescue = new RescueTask(thread);
                    thread.triggerBreakdown(stateChange.getBreakdownType());
                    try {
                        ordersScheduled.put(rescue);
                    } catch (InterruptedException ignore) {}
                }
                newState.put(thread.getVehicleId(), thread);
            }
        }
    }
}
