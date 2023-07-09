package com.trackerhub.backend.services;

import com.trackerhub.orders.*;
import com.trackerhub.scheduling.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.awt.*;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class OrderScheduler {
    private final static Logger log = LoggerFactory.getLogger(OrderScheduler.class);
    private final InputDataReader inputDataReader;

    public static LocalDateTime timer;

    public static AtomicBoolean collapsed = new AtomicBoolean(false);

    private LocalDateTime dateLimit;

    private final Map<VehicleType, Map<VehicleState, Map<String, VehicleThread>>> vehicleThreads;

    private final BlockingQueue<VehicleStateChange> vehicleStateChanges;

    private final BlockingQueue<OrderLine> ordersReceived;

    private final BlockingQueue<OrderTask> ordersScheduled;

    private VehicleStateHandler vehicleStateHandler;
    private OrderTaskFactory orderTaskFactory;
    private OrderTaskScheduler orderTaskScheduler;

    private Point origin;
    private int maxX;
    private int maxY;
    private int timeRatio;



    private String status;

    public OrderScheduler(InputDataReader inputDataReader) {
        vehicleThreads = new ConcurrentHashMap<>();
        vehicleStateChanges = new LinkedBlockingQueue<>();
        ordersReceived = new PriorityBlockingQueue<>();
        ordersScheduled = new PriorityBlockingQueue<>();
        this.inputDataReader = inputDataReader;
    }

    public String getStatus() {
        return this.status;
    }

    public Map<String, VehicleThread> getVehicleThreads(VehicleType type, VehicleState state) {
        synchronized (vehicleThreads) {
            return Map.copyOf(vehicleThreads.get(type).get(state));
        }
    }

    public void triggerVehicleBreakdown(String vehicleId, int breakdownType) throws InterruptedException {
        VehicleType type = VehicleType.CAR;
        if (vehicleId.contains("Mot")) {
            type = VehicleType.MOTORCYCLE;
        }
        var vehicle = vehicleThreads.get(type).get(VehicleState.BUSY).get(vehicleId);
        if (vehicle == null) {
            return;
        }
        vehicleStateChanges.put(new VehicleStateChange(VehicleState.BUSY, VehicleState.DAMAGED, type, vehicleId, breakdownType));
    }

    public void setWarehousePosition(int x, int y) {
        this.origin = new Point(x, y);
    }

    public void setMapSize(int maxX, int maxY) {
        this.maxX = maxX;
        this.maxY = maxY;
    }

    public void initialize() {
        orderTaskFactory = new OrderTaskFactory(8, ordersReceived, ordersScheduled);
        vehicleStateHandler = new VehicleStateHandler(vehicleStateChanges, vehicleThreads, ordersScheduled);
        orderTaskScheduler = new OrderTaskScheduler(vehicleThreads, ordersScheduled, vehicleStateChanges);
        vehicleStateHandler.start();
        orderTaskFactory.start();
        orderTaskScheduler.start();
        log.info("Date: " + timer);
        inputDataReader.setBreakdownsFromFile();
        status = "running";
    }

    public void setDate(String newDate, int simulationDays) {
        timer = LocalDate.parse(newDate, DateTimeFormatter.ofPattern("dd/MM/yyyy")).atStartOfDay();
        if (simulationDays > 0) {
            dateLimit = LocalDateTime.of(timer.toLocalDate(), timer.toLocalTime()).plusDays(simulationDays);
        }
        inputDataReader.setOrdersFromFile(timer);
        inputDataReader.setBlocksFromFile(timer);
    }

    public void stopSimulation() {
        vehicleStateHandler.interrupt();
        orderTaskFactory.interrupt();
        orderTaskScheduler.interrupt();
        try {
            vehicleStateHandler.join();
            orderTaskFactory.join();
            orderTaskScheduler.join();
        } catch (InterruptedException ignore) {}
        ordersScheduled.clear();
        ordersReceived.clear();
        stopSimulationHelper(VehicleType.MOTORCYCLE, VehicleState.DAMAGED);
        stopSimulationHelper(VehicleType.MOTORCYCLE, VehicleState.BUSY);
        stopSimulationHelper(VehicleType.MOTORCYCLE, VehicleState.AVAILABLE);
        stopSimulationHelper(VehicleType.CAR, VehicleState.DAMAGED);
        stopSimulationHelper(VehicleType.CAR, VehicleState.BUSY);
        stopSimulationHelper(VehicleType.CAR, VehicleState.AVAILABLE);
        status = "stopped";
    }

    private void simulationCollapsed() {
        stopSimulation();
        ordersScheduled.clear();
        status = "collapse";
    }

    private void stopSimulationHelper(VehicleType type, VehicleState state) {
        var vehicles = vehicleThreads.get(type).get(state);
        vehicles.forEach((key, value) -> {
            value.interrupt();
            try {
                value.join();
            } catch (InterruptedException ignore) {}
        });
    }

    public void setDate() {
        timer = LocalDateTime.now();
        inputDataReader.setBlocksFromFile(timer);
    }

    private void initializeVehicleThreads(VehicleDescription desc, int num) {
        vehicleThreads.get(desc.getType()).put(VehicleState.AVAILABLE, new ConcurrentHashMap<>());
        vehicleThreads.get(desc.getType()).put(VehicleState.BUSY, new ConcurrentHashMap<>());
        vehicleThreads.get(desc.getType()).put(VehicleState.DAMAGED, new ConcurrentHashMap<>());
        for (int i = 0; i < num; i++) {
            String id = String.format("%s%03d", desc.getType(), i + 1);
            var thread = new VehicleThread(id, desc, maxX, maxY, origin, inputDataReader, vehicleStateChanges, timeRatio);
            vehicleThreads.get(desc.getType()).get(VehicleState.AVAILABLE).put(thread.getVehicleId(), thread);
            thread.start();
        }
    }

    public void initializeThreads(int numCars, int numBikes, int timeRatio) {
        vehicleThreads.clear();
        this.timeRatio = timeRatio;
        var car = new VehicleDescription(VehicleType.CAR, 40, 60);
        var bike = new VehicleDescription(VehicleType.MOTORCYCLE, 8, 30);
        vehicleThreads.put(VehicleType.CAR, new ConcurrentHashMap<>());
        vehicleThreads.put(VehicleType.MOTORCYCLE, new ConcurrentHashMap<>());
        initializeVehicleThreads(car, numCars);
        initializeVehicleThreads(bike, numBikes);
    }

    public void addNewDailyOrder(OrderLine order) throws InterruptedException {
        ordersReceived.put(order);
    }

    public void addOrdersFromFile(InputStream inputStream) {
        inputDataReader.setOrdersFromStream(timer, inputStream);
    }

    public void bulkLoadBreakdowns(List<BreakdownLine> breakdownLine) {
        inputDataReader.bulkLoadBreakdowns(breakdownLine);
    }

    public InputDataReader getInputDataReader() {
        return inputDataReader;
    }

    @Scheduled(fixedRate = 1000)
    private void timer() throws InterruptedException {
        if (timer == null) return;
        if (collapsed.get()) {
            simulationCollapsed();
            timer = null;
            return;
        }
        if (dateLimit != null) {
            if (timer.isAfter(dateLimit)) {
                timer = null;
                stopSimulation();
                return;
            }
        }
        if (timeRatio == 1) {
            timer = timer.plusSeconds(1);
        } else if (timeRatio == 60) {
            timer = timer.plusMinutes(1);
        } else if (timeRatio == 600) {
            timer = timer.plusMinutes(10);
        } else if (timeRatio == 6000) {
            timer = timer.plusMinutes(100);
        }
        if (dateLimit != null) {
            if (timer.isAfter(dateLimit)) {
                timer = null;
                stopSimulation();
                return;
            }
        }
        if ((inputDataReader.getCurDate() != null) && timer.toLocalDate() != inputDataReader.getCurDate().toLocalDate()) {
            log.info("Date: " + timer);
            inputDataReader.setOrdersFromFile(timer);
            inputDataReader.reloadBreakdowns();
        }
        var orders = inputDataReader.getOrderAtTime(timer);
        for (var order: orders) {
            ordersReceived.put(order);
        }
    }
}
