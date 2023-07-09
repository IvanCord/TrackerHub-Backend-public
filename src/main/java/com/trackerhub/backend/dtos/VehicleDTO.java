package com.trackerhub.backend.dtos;

import com.trackerhub.scheduling.VehicleState;
import com.trackerhub.scheduling.VehicleType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class VehicleDTO {
    String id;
    VehicleType type;
    VehicleState state;
    int breakdownType;
    CoordinatesDTO coord;
    List<CoordinatesDTO> plannedPath;
    List<TaskDTO> tasks;
}
