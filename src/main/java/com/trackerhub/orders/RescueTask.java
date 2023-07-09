package com.trackerhub.orders;

import com.trackerhub.scheduling.VehicleThread;
import com.trackerhub.scheduling.VehicleType;
import lombok.Getter;

import java.awt.*;
import java.awt.geom.Point2D;
import java.time.LocalDateTime;

@Getter
public class RescueTask extends OrderTask {
    private final VehicleThread vehicleThread;

    public Point2D.Double getRescueCoords() {
        return new Point2D.Double(vehicleThread.getCurrentNode().getPoint().x,
                vehicleThread.getCurrentNode().getPoint().y);
    }

    @Override
    public boolean isRescue() {
        return true;
    }

    public RescueTask(VehicleThread vehicleThread) {
        this.vehicleThread = vehicleThread;
        this.order = new OrderLine();
        order.setDestination(new Point((int)getRescueCoords().x, (int)getRescueCoords().y));
        var unixDate = LocalDateTime.of(1970, 1, 1, 0, 0);
        order.setDateLimit(unixDate);
        if (vehicleThread.getVehicleType() == VehicleType.CAR) {
            numPackages = 40;
        } else {
            numPackages = 8;
        }
    }
}
