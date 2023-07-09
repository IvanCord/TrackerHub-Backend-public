package com.trackerhub.backend.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class NewDailyOrderDTO {
    int posX;
    int posY;
    int numPaq;
    int hoursLimit;
}
