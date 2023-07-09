package com.trackerhub.backend.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@AllArgsConstructor
public class TaskDTO {
    CoordinatesDTO destination;
    int numPackages;
    LocalDateTime dateLimit;
    long clientId;
}
