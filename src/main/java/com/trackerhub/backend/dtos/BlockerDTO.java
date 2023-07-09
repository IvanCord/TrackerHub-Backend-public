package com.trackerhub.backend.dtos;

import com.trackerhub.orders.BlockLine;
import lombok.Getter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
public class BlockerDTO {
    // private final UUID id;
    private final LocalDateTime init;
    private final LocalDateTime fin;
    private final List<CoordinatesDTO> coords;

    public BlockerDTO(BlockLine line) {
        coords = new ArrayList<>();
        init = line.getFrom();
        fin = line.getUntil();
        line.getCoordinates().forEach(point -> coords.add(new CoordinatesDTO(point)));
    }
}
