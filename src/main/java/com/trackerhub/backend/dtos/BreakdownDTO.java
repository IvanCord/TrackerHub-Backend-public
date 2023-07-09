package com.trackerhub.backend.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class BreakdownDTO {
    String vehicleId;
    // 1 -> incidente simple, inicia demora de 2 horas
    // 2 -> indicente medio, inicia demora de 1 turno + el restante del turno actual
    // 3 -> indicente grave, demora de 2 dias enteros
    int type;
}
