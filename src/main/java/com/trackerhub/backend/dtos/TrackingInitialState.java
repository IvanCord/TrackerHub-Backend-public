package com.trackerhub.backend.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TrackingInitialState {
    private int mapMaxX;
    private int mapMaxY;
    private int warehouseX;
    private int warehouseY;
    private int numCars;
    private int numMotorcycles;
    private String date;
    // dias que tienen que pasar antes que se detenga la simulacion
    // -1 para simular hasta el colapso
    private int simulationDays;
    // 1 -> 1 seg real = 1 seg simulacion
    // 60 -> 1 seg real = 1 min simulacion
    // 6000 -> 1 seg real = 10 min simulacion
    private int timeRatio;
}
