package com.trackerhub.backend.components;

import com.trackerhub.backend.dtos.*;
import com.trackerhub.backend.services.OrderScheduler;
import com.trackerhub.orders.BreakdownLine;
import com.trackerhub.orders.OrderLine;
import com.trackerhub.scheduling.VehicleState;
import com.trackerhub.scheduling.VehicleType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@RequestMapping("/api/v1/tracking")
@RestController
public class Tracking {
    private final OrderScheduler orderScheduler;

    public Tracking(OrderScheduler orderScheduler) {
        this.orderScheduler = orderScheduler;
    }

    @GetMapping("/time")
    public ResponseEntity<LocalDateTime> getSystemDate() {
        return ResponseEntity.ofNullable(OrderScheduler.timer);
    }

    @PostMapping("/setup")
    public ResponseEntity<Boolean> setupInitialState(@RequestBody TrackingInitialState state) {
        orderScheduler.setMapSize(state.getMapMaxX(), state.getMapMaxY());
        orderScheduler.setWarehousePosition(state.getWarehouseX(), state.getWarehouseY());
        orderScheduler.initializeThreads(state.getNumCars(), state.getNumMotorcycles(), state.getTimeRatio());
        if (state.getDate().isEmpty()) {
            orderScheduler.setDate();
        } else {
            orderScheduler.setDate(state.getDate(), state.getSimulationDays());
        }
        orderScheduler.initialize();
        return ResponseEntity.ok(true);
    }

    private void setVehicleTrackingStatus(List<VehicleDTO> vehicles, VehicleType type, VehicleState state) {
        var entries = orderScheduler.getVehicleThreads(type, state);
        entries.forEach((id, thread) -> {
            var tasks = thread.getVehicleTasks();
            var vehicle = new VehicleDTO(id, type, state, thread.getBreakdownType(),
                    new CoordinatesDTO(thread.getCurrentPosition()), new ArrayList<>(), new ArrayList<>());
            for (var node: thread.getPlannedPathList()) {
                vehicle.getPlannedPath().add(new CoordinatesDTO(node.getPoint()));
            }
            tasks.forEach(task -> vehicle.getTasks().add(new TaskDTO(
                    new CoordinatesDTO(task.getOrder().getDestination()),
                    task.getNumPackages(),
                    task.getOrder().getDateLimit(),
                    task.getOrder().getClientId()))
            );
            vehicles.add(vehicle);
        });
    }

    @PostMapping("/orders/new")
    public ResponseEntity<OrderLine> addNewOrder(@RequestBody NewDailyOrderDTO order) throws InterruptedException {
        var line = new OrderLine(order);
        orderScheduler.addNewDailyOrder(line);
        return ResponseEntity.ok(line);
    }

    @PostMapping("/{fileType}/bulk")
    public void loadBulkData(@RequestParam("file") MultipartFile file, @PathVariable String fileType) throws IOException {
        var is = file.getInputStream();
        if (Objects.equals(fileType, "orders")) {
            orderScheduler.addOrdersFromFile(is);
            return;
        }
        if (!Objects.equals(fileType, "breakdowns")) {
            return;
        }
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        var breakdowns = new ArrayList<BreakdownLine>();
        while ((line = br.readLine()) != null) {
            var breakdown = new BreakdownLine(line);
            breakdowns.add(breakdown);
        }
        orderScheduler.bulkLoadBreakdowns(breakdowns);
    }

    @GetMapping("/status")
    public ResponseEntity<TrackingStatusDTO> getTrackingStatus() {
        var result = new TrackingStatusDTO();
        setVehicleTrackingStatus(result.getMotos(), VehicleType.MOTORCYCLE, VehicleState.AVAILABLE);
        setVehicleTrackingStatus(result.getMotos(), VehicleType.MOTORCYCLE, VehicleState.BUSY);
        setVehicleTrackingStatus(result.getMotos(), VehicleType.MOTORCYCLE, VehicleState.DAMAGED);
        setVehicleTrackingStatus(result.getAutos(), VehicleType.CAR, VehicleState.AVAILABLE);
        setVehicleTrackingStatus(result.getAutos(), VehicleType.CAR, VehicleState.BUSY);
        setVehicleTrackingStatus(result.getAutos(), VehicleType.CAR, VehicleState.DAMAGED);
        if (OrderScheduler.timer != null) {
            var blocks = orderScheduler.getInputDataReader().getBlockLinesAtTime(OrderScheduler.timer);
            blocks.forEach(blockLine -> result.getBlockers().add(new BlockerDTO(blockLine)));
        }
        result.setStatus(orderScheduler.getStatus());
        return ResponseEntity.ok(result);
    }

    @PostMapping("/stop")
    public ResponseEntity<Boolean> stopSimulation() {
        orderScheduler.stopSimulation();
        return ResponseEntity.ok(true);
    }

    @GetMapping("/status/blocks")
    public ResponseEntity<List<BlockerDTO>> getBlockerStatus() {
        var result = new ArrayList<BlockerDTO>();
        var blocks = orderScheduler.getInputDataReader().getBlockLinesAtTime(OrderScheduler.timer);
        blocks.forEach(blockLine -> result.add(new BlockerDTO(blockLine)));
        return ResponseEntity.ok(result);
    }

    @PostMapping("/breakdown")
    public ResponseEntity<Boolean> triggerBreakdown(@RequestBody BreakdownDTO breakdown) throws InterruptedException {
        orderScheduler.triggerVehicleBreakdown(breakdown.getVehicleId(), breakdown.getType());
        return ResponseEntity.ok(true);
    }
}
