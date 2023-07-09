package com.trackerhub.scheduling;

import com.trackerhub.algorithms.AStar;
import com.trackerhub.algorithms.Node;
import com.trackerhub.backend.services.InputDataReader;
import com.trackerhub.backend.services.OrderScheduler;
import com.trackerhub.backend.services.TurnsUtil;
import com.trackerhub.orders.BreakdownLine;
import com.trackerhub.orders.OrderLine;
import com.trackerhub.orders.OrderTask;
import com.trackerhub.orders.RescueTask;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
public class VehicleThread extends Thread {
    private final static Logger log = LoggerFactory.getLogger(VehicleThread.class);
    private final VehicleDescription vehicleDescription;
    private final VehicleType vehicleType;
    private final String vehicleId;
    private final BlockingQueue<OrderTask> tasks;

    private final Node origin;
    private final BlockingQueue<VehicleStateChange> stateChanges;
    private final InputDataReader inputDataReader;
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private final int timeRatio;
    private final int speedMultiplier;
    private final int sleepTime;
    private final Set<Node> plannedPath;

    private Node currentNode;
    private Node previousNode = null;
    private Node nextNode;
    private Node destination;

    private Node breakdownTarget;
    private BreakdownLine breakdownLine;

    private Point2D.Double destPoint;
    private Point originPoint;

    private OrderTask currentTask;

    private Point2D.Double nextPosition;
    private Point2D.Double currentPosition;
    private Point2D.Double previousPosition;
    private Point2D.Double direction;

    private final AStar algo;
    private Set<Point> blocks;
    private Set<Point> additionalBlocks;

    private double speedms;

    private AtomicBoolean brokeDown = new AtomicBoolean(false);
    private int breakdownType = 0;
    private TurnsUtil turns = new TurnsUtil();
    private boolean rescueTask = false;

    public VehicleThread(String vehicleId, VehicleDescription vehicleDescription, int maxX, int maxY,
                         Point origin, InputDataReader inputDataReader,
                         BlockingQueue<VehicleStateChange> stateChanges, int timeRatio) {
        this.vehicleId = vehicleId;
        this.vehicleDescription = vehicleDescription;
        vehicleType = vehicleDescription.getType();
        this.tasks = new LinkedBlockingQueue<>();
        this.algo = new AStar(maxX, maxY);
        originPoint = origin;
        this.origin = algo.getNode(originPoint).orElseThrow();
        this.stateChanges = stateChanges;
        this.inputDataReader = inputDataReader;
        this.plannedPath = Collections.synchronizedSet(new LinkedHashSet<>());

        this.currentPosition = new Point2D.Double(origin.x, origin.y);
        this.nextPosition = new Point2D.Double(origin.x, origin.y);
        this.previousPosition = new Point2D.Double();
        this.direction = new Point2D.Double();
        this.timeRatio = timeRatio;
        this.blocks = new HashSet<>();
        this.additionalBlocks = new HashSet<>();

        speedms = vehicleDescription.getSpeed() / 3.6;
        if (timeRatio >= 60) {
            speedMultiplier = 60;
        } else {
            speedMultiplier = 1;
        }
        if (timeRatio == 6000) {
            sleepTime = 10;
        } else if (timeRatio == 600) {
            sleepTime = 100;
        } else {
            sleepTime = 1000;
        }
    }

    @Override
    public void run() {
        while (!shouldStop.get()) {
            try {
                if (currentTask == null) {
                    currentTask = tasks.take();
                    rescueTask = currentTask.isRescue();
                    if (rescueTask) {
                        destPoint = ((RescueTask) currentTask).getRescueCoords();
                        var destVehicle = ((RescueTask) currentTask).getVehicleThread();
                        log.info(vehicleId + " going to rescue vehicle " + destVehicle.vehicleId);
                    } else {
                        destPoint = new Point2D.Double(currentTask.getOrder().getDestination().x,
                                currentTask.getOrder().getDestination().y);
                    }
                    destination = algo.getNode(destPoint).orElseThrow();
                }
                // no verificar task de retorno o de rescate
                boolean isSpecialTask = (currentTask.getNumPackages() == 0) || rescueTask;
                if (!isSpecialTask && OrderScheduler.timer.isAfter(currentTask.getOrder().getDateLimit())) {
                    System.out.println("Order " + currentTask.getOrder().getOrderId() + " didn't arrive at " + currentTask.getOrder().getDateLimit());
                    System.out.println("Vehicle " + vehicleId);
                    System.out.println("Destination " + currentTask.getOrder().getDestination());
                    System.out.println("Current time " + OrderScheduler.timer);
                    System.out.println("Planned path " + plannedPath.size());
                    var blocks = inputDataReader.getBlocksAtTime(currentTask.getOrder().getDateLimit());
                    blocks.forEach(System.out::println);
                    stateChanges.put(new VehicleStateChange(null, null, vehicleType, vehicleId));
                    return;
                }
                // el hilo solo duerme y trata de continuar con su bucle
                // algo externo debe de interactuar con sus ordenes
                if (brokeDown.get()) {
                    brokeDown.set(false);
                    var breakDownSleep = turns.getDelayDuration(breakdownType) / timeRatio;
                    Thread.sleep(breakDownSleep);
                    plannedPath.clear();
                    currentTask = null;
                    tasks.clear();
                    if (breakdownType == 1) {
                        stateChanges.put(new VehicleStateChange(VehicleState.DAMAGED, VehicleState.BUSY, vehicleType, vehicleId));
                        var returnOrder = new OrderLine();
                        returnOrder.setDestination(originPoint);
                        var returnTask = new OrderTask(returnOrder, 0);
                        tasks.put(returnTask);
                    } else if (breakdownType >= 2) {
                        stateChanges.put(new VehicleStateChange(VehicleState.DAMAGED, VehicleState.AVAILABLE, vehicleType, vehicleId));
                        currentPosition.setLocation(originPoint);
                        nextPosition.setLocation(originPoint);
                    }
                    continue;
                }
                verifyNodeArrival();
                if (currentTask != null) {
                    currentPosition.setLocation(
                            currentPosition.x + ((direction.x * speedms) / 1000) * speedMultiplier,
                            currentPosition.y + ((direction.y * speedms) / 1000) * speedMultiplier
                    );
                }
                if (blocks.contains(new Point((int)Math.ceil(currentPosition.x), (int)Math.ceil(currentPosition.y)))) {
                    additionalBlocks.add(
                            new Point((int)(currentPosition.x + direction.x), (int)(currentPosition.y + direction.y))
                    );
                }
                verifyNodeArrival();
                if (!isSpecialTask && plannedPath.size() >= 10) {
                    var breakdown = inputDataReader.vehicleHasScheduledBreakdown(vehicleId);
                    if (breakdown.isPresent()) {
                        double random = ThreadLocalRandom.current().nextInt(5, 36) / 100.0;
                        breakdownTarget = plannedPath.stream().toList().get((int)Math.ceil(plannedPath.size() * random));
                        breakdownLine = breakdown.get();
                        log.info("Vehicle " + vehicleId + " got breakdown type " + breakdownLine.getType());
                    }
                }
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void verifyNodeArrival() throws InterruptedException {
        if (currentTask == null) {
            return;
        }
        if (Math.abs(currentPosition.x - nextPosition.x) > 0.000001 || Math.abs(currentPosition.y - nextPosition.y) > 0.000001) {
            return;
        }
        currentPosition.setLocation(nextPosition);
        currentNode = algo.getNode(currentPosition).orElseThrow();
        if (breakdownTarget != null && (currentNode == breakdownTarget)) {
            stateChanges.put(new VehicleStateChange(VehicleState.BUSY, VehicleState.DAMAGED, vehicleType, vehicleId, breakdownLine.getType()));
            breakdownTarget = null;
        }
        if (currentNode == destination) {
            // verificar si estamos salvando a un hilo
            if (rescueTask) {
                tasks.clear();
                var destVehicle = ((RescueTask) currentTask).getVehicleThread();
                extractTasks(destVehicle, tasks);
                currentTask = null;
                return;
            }
            // descargar todos los nodos
            currentTask = tasks.peek();
            while (currentTask != null) {
                var destTmp = new Point2D.Double(currentTask.getOrder().getDestination().x, currentTask.getOrder().getDestination().y);
                if (destTmp.equals(destPoint)) {
                    tasks.poll();
                    currentTask = tasks.peek();
                } else {
                    currentTask = null;
                }
            }
            if (tasks.isEmpty()) {
                stateChanges.put(new VehicleStateChange(VehicleState.BUSY, VehicleState.AVAILABLE, vehicleType, vehicleId));
            }
            return;
        }
        blocks = inputDataReader.getBlocksAtTime(OrderScheduler.timer);
        if (!additionalBlocks.isEmpty()) {
            blocks.addAll(additionalBlocks);
            additionalBlocks.clear();
        }
        synchronized (plannedPath) {
            if (plannedPath.isEmpty()) {
                blocks.forEach(point -> algo.getNode(point).orElseThrow().setG(Integer.MAX_VALUE));
                var path = algo.getShortestPathList(currentNode, destination);
                plannedPath.addAll(path);
                algo.cleanNodes();
            }
            // check if blocks intersect with planned path
            if (pathAndBlocksIntersect(blocks)) {
                plannedPath.clear();
                blocks.forEach(point -> algo.getNode(point).orElseThrow().setG(Integer.MAX_VALUE));
                var path = algo.getShortestPathList(currentNode, destination);
                plannedPath.addAll(path);
                algo.cleanNodes();
            }

            var iter = plannedPath.iterator();
            previousNode = iter.next();
            try {
                nextNode = iter.next();
            } catch (NoSuchElementException ignore) {
                plannedPath.clear();
                blocks.forEach(point -> algo.getNode(point).orElseThrow().setG(Integer.MAX_VALUE));
                var path = algo.getShortestPathList(currentNode, destination);
                plannedPath.addAll(path);
                algo.cleanNodes();
                iter = plannedPath.iterator();
                previousNode = iter.next();
                nextNode = iter.next();
            }
            plannedPath.remove(previousNode);
        }

        previousPosition.setLocation(previousNode.getPoint().x, previousNode.getPoint().y);
        nextPosition.setLocation(nextNode.getPoint().x, nextNode.getPoint().y);
        direction.setLocation(nextPosition.x - previousPosition.x, nextPosition.y - previousPosition.y);
    }

    private boolean pathAndBlocksIntersect(Set<Point> blocks) {
        for (var node: plannedPath) {
            if (blocks.contains(node.getPoint())) {
                return true;
            }
        }
        return false;
    }

    public List<Node> getPlannedPathList() {
        synchronized (plannedPath) {
            return List.copyOf(plannedPath);
        }
    }

    public List<OrderTask> getVehicleTasks() {
        var vTasks = new ArrayList<OrderTask>();
        if (currentTask != null) {
            vTasks.add(currentTask);
        }
        vTasks.addAll(List.copyOf(tasks));
        return vTasks;
    }

    private void extractTasks(VehicleThread source, BlockingQueue<OrderTask> dest) throws InterruptedException {
        if (source.getCurrentTask() != null) {
            dest.put(source.getCurrentTask());
            source.setCurrentTask(null);
        }
        source.getTasks().drainTo(dest);
    }

    public void assignTask(Queue<OrderTask> newTasks) {
        // regresar al origen
        var returnOrder = new OrderLine();
        returnOrder.setDestination(originPoint);
        var returnTask = new OrderTask(returnOrder, 0);
        try {
            for (var task: newTasks) {
                tasks.put(task);
            }
            tasks.put(returnTask);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void triggerBreakdown(int type) {
        brokeDown.set(true);
        breakdownType = type;
    }

    public int getBreakdownType() {
        return this.breakdownType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VehicleThread that)) return false;

        return vehicleId.equals(that.vehicleId);
    }

    @Override
    public int hashCode() {
        return vehicleId.hashCode();
    }
}
