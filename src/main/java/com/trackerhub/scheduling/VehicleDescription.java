package com.trackerhub.scheduling;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class VehicleDescription {
    private UUID id = UUID.randomUUID();

    private VehicleType type;

    private int capacity;

    private float speed; // km/m

    public VehicleDescription(VehicleType type, int capacity, float speed) {
        this.type = type;
        this.capacity = capacity;
        this.speed = speed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VehicleDescription that)) return false;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
