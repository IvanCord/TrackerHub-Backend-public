package com.trackerhub.scheduling;

import lombok.AllArgsConstructor;
import lombok.Getter;


@Getter
public class VehicleStateChange {
    private final VehicleState oldState;

    private final VehicleState newState;

    private final VehicleType type;

    private final String threadId;

    private final int breakdownType;

    public VehicleStateChange(VehicleState oldState, VehicleState newState, VehicleType type, String threadId) {
        this.oldState = oldState;
        this.newState = newState;
        this.type = type;
        this.threadId = threadId;
        this.breakdownType = 0;
    }

    public VehicleStateChange(VehicleState oldState, VehicleState newState, VehicleType type, String threadId, int breakdownType) {
        this.oldState = oldState;
        this.newState = newState;
        this.type = type;
        this.threadId = threadId;
        this.breakdownType = breakdownType;
    }
}
