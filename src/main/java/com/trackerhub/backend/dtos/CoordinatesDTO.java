package com.trackerhub.backend.dtos;

import lombok.Getter;

import java.awt.*;
import java.awt.geom.Point2D;

@Getter
public class CoordinatesDTO {
    private final double x;
    private final double y;

    public CoordinatesDTO(Point2D.Double point) {
        x = point.x;
        y = point.y;
    }

    public CoordinatesDTO(Point point) {
        x = point.x;
        y = point.y;
    }
}
