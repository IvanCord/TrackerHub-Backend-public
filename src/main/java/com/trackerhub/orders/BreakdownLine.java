package com.trackerhub.orders;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class BreakdownLine {
    private int turno;
    private int type;
    private String vehicleId;

    public BreakdownLine(String input) {
        var parts = input.split("_");
        vehicleId = parts[1];
        turno = Integer.parseInt(parts[0].split("")[1]);
        type = Integer.parseInt(parts[2].split("")[2]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BreakdownLine that)) return false;

        return vehicleId.equals(that.vehicleId);
    }

    @Override
    public int hashCode() {
        return vehicleId.hashCode();
    }
}
