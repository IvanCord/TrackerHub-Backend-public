package com.trackerhub.scheduling;

public enum VehicleType {
    CAR ("Aut"),
    MOTORCYCLE ("Mot");

    private final String name;

    VehicleType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
