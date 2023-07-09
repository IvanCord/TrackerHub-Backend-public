package com.trackerhub.backend.services;

import com.trackerhub.orders.BlockLine;
import com.trackerhub.orders.BreakdownLine;
import com.trackerhub.orders.OrderLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.awt.*;
import java.io.*;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class InputDataReader {
    private final static Logger log = LoggerFactory.getLogger(InputDataReader.class);
    private final ResourceLoader resourceLoader;

    private final Queue<OrderLine> orders;

    private List<BlockLine> blocks;

    private final Map<Integer, Map<String, BreakdownLine>> originalBreakdowns;

    private Map<Integer, Map<String, BreakdownLine>> breakdowns;

    private LocalDateTime curDate = null;

    public InputDataReader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
        this.originalBreakdowns = new ConcurrentHashMap<>();
        this.breakdowns = new ConcurrentHashMap<>();
        this.orders = new LinkedList<>();
    }

    public void setOrdersFromFile(LocalDateTime date) {
        var format = DateTimeFormatter.ofPattern("yyyyMMdd");
        var filename = String.format("classpath:pedidos/pedido%s.txt", date.format(format));
        var file = resourceLoader.getResource(filename);
        curDate = date;
        try {
            setOrdersFromStream(date, file.getInputStream());
        } catch (IOException ignore) {}
    }

    public void setOrdersFromStream(LocalDateTime date, InputStream stream) {
        try (var br = new BufferedReader(new InputStreamReader(stream))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    var order = new OrderLine(date, line);
                    orders.add(order);
                } catch (DateTimeException ignore) {}
            }
        } catch (IOException ignored) {}
    }

    public void setBreakdownsFromFile() {
        originalBreakdowns.clear();
        var file = resourceLoader.getResource("classpath:averias.txt");
        try (var br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                var breakdown = new BreakdownLine(line);
                originalBreakdowns.computeIfAbsent(breakdown.getTurno(), k -> new ConcurrentHashMap<>());
                originalBreakdowns.get(breakdown.getTurno()).put(breakdown.getVehicleId(), breakdown);
            }
        } catch (IOException ignored) {}
        reloadBreakdowns();
    }

    public void bulkLoadBreakdowns(List<BreakdownLine> breakdownList) {
        originalBreakdowns.clear();
        breakdownList.forEach(breakdownLine -> {
            originalBreakdowns.computeIfAbsent(breakdownLine.getTurno(), k -> new ConcurrentHashMap<>());
            originalBreakdowns.get(breakdownLine.getTurno()).put(breakdownLine.getVehicleId(), breakdownLine);
        });
        reloadBreakdowns();
    }

    public void reloadBreakdowns() {
        log.info("Loading breakdowns");
        breakdowns = originalBreakdowns.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new ConcurrentHashMap<>(e.getValue())));
    }

    public Optional<BreakdownLine> vehicleHasScheduledBreakdown(String vehicleId) {
        TurnsUtil turns = new TurnsUtil();
        var turnMap = breakdowns.get(turns.getCurrentTurn());
        if (turnMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(turnMap.remove(vehicleId));
    }

    public void setBlocksFromFile(LocalDateTime date) {
        var format = DateTimeFormatter.ofPattern("yyyyMM");
        var filename = String.format("classpath:bloqueos/%sbloqueadas.txt", date.format(format));
        var file = resourceLoader.getResource(filename);
        blocks = new ArrayList<>();
        try (var br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                var block = new BlockLine(line, date.toLocalDate());
                blocks.add(block);
            }
        } catch (IOException ignore) {}
    }

    public LocalDateTime getCurDate() {
        return curDate;
    }

    public List<OrderLine> getOrderAtTime(LocalDateTime date) {
        var res = new ArrayList<OrderLine>();
        var order = orders.peek();
        if (order == null) {
            return res;
        }
        while (order != null && !order.getDateArrival().isAfter(date)) {
            res.add(orders.poll());
            order = orders.peek();
        }
        return res;
    }

    public Set<Point> getBlocksAtTime(LocalDateTime time) {
        var temp = new ArrayList<Point>();
        blocks.forEach(blockLine -> {
            if (time.isAfter(blockLine.getFrom()) && time.isBefore(blockLine.getUntil())) {
                temp.addAll(blockLine.getCoordinates());
            }
        });
        return getPointsInBetween(temp);
    }

    public List<BlockLine> getBlockLinesAtTime(LocalDateTime time) {
        var result = new ArrayList<BlockLine>();
        blocks.forEach(blockLine -> {
            if (time.isAfter(blockLine.getFrom()) && time.isBefore(blockLine.getUntil())) {
                blockLine.setCoordinates(getPointsInBetween(blockLine.getCoordinates()).stream().toList());
                result.add(blockLine);
            }
        });
        return result;
    }

    public Set<Point> getPointsInBetween(List<Point> coords) {
        var result = new LinkedHashSet<Point>();
        var it = coords.iterator();
        if (!it.hasNext()) {
            return result;
        }
        Point c2 = null;
        Point c1;
        while (true) {
            if (c2 == null) {
                c1 = it.next();
            } else {
                c1 = new Point(c2);
            }
            c2 = null;
            if (it.hasNext()) {
                c2 = it.next();
            }
            if (c2 == null) break;

            // calculate direction
            int newX = 0;
            try {
                newX = (c2.x - c1.x) / Math.abs(c2.x - c1.x);
            } catch (ArithmeticException ignore) {}
            int newY = 0;
            try {
                newY = (c2.y - c1.y) / Math.abs(c2.y - c1.y);
            } catch (ArithmeticException ignore) {}
            Point direction = new Point(newX, newY);

            // calculate number of points in between
            int numPoints = 0;
            if (newX == 0) {
                numPoints = Math.abs(c2.y - c1.y);
            } else if (newY == 0) {
                numPoints = Math.abs(c2.x - c1.x);
            }

            // add all points
            Point runner = new Point(c1);
            for (int i = 0; i <= numPoints; i++) {
                result.add(new Point(runner));
                runner.setLocation(runner.x + direction.x, runner.y + direction.y);
            }
        }
        return result;
    }
}
