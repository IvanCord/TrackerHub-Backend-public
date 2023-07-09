package com.trackerhub.backend.dtos;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class TrackingStatusDTO {
    private final List<VehicleDTO> autos = new ArrayList<>();
    private final List<VehicleDTO> motos = new ArrayList<>();
    private final List<BlockerDTO> blockers = new ArrayList<>();
    private String status;
}
