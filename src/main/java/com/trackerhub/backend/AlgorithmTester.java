package com.trackerhub.backend;

import com.trackerhub.backend.services.OrderScheduler;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class AlgorithmTester implements ApplicationRunner {
//    private final OrderScheduler orderScheduler;

//    public AlgorithmTester(OrderScheduler orderScheduler) {
//        this.orderScheduler = orderScheduler;
//    }

    @Override
    public void run(ApplicationArguments args) throws InterruptedException {
//        var algo = new Dijkstra();
//        var algo = new AStar();
//        algo.initializeNodeMap(70, 50);

//        var warehouse = algo.getNode(45, 30).orElseThrow();

//        orderScheduler.setMapSize(70, 50);
//        orderScheduler.setWarehousePosition(45, 30);
//        orderScheduler.initializeThreads(40, 60);
//        orderScheduler.setDate("18/06/2024");
//        orderScheduler.initialize();

        /*
        var date = LocalDate.parse("05/02/2023", DateTimeFormatter.ofPattern("dd/MM/yyyy")).atStartOfDay();
        var orders = inputDataReader.getOrdersFromFileBenchmark(date);
        while (orders.size() != 0) {
            System.out.printf("Date: %s, number of orders: %d\n", date, orders.size());
            date = date.plusDays(1);
            orders = inputDataReader.getOrdersFromFileBenchmark(date);
        }*/

        // 600 -> 2024-06-18 (554)
        // 400 -> 2024-02-26 (396)
        // 200 -> 2023-08-31 (200)
        // 100 -> 2023-05-20 (110)

        // generar bloqueos
//        var rand = new Random();
//        int xBound = 71, yBound = 51;
//        var blocks = new HashSet<Point>();
//        while (blocks.size() <= 50) {
//            var p = new Point(rand.nextInt(xBound), rand.nextInt(yBound));
//            blocks.add(p);
//        }
//
//        var dates = List.of("20/05/2023", "31/08/2023", "26/02/2024", "18/06/2024");
//
//        System.out.println("Dijkstra algorithm");
//        var algo = new AStar();
//        algo.initializeNodeMap(70, 50);
//        var warehouse = algo.getNode(45, 30).orElseThrow();
//        blocks.forEach(point -> algo.getNode(point).orElseThrow().setG(Integer.MAX_VALUE));
//        var dest = algo.getNode(0, 0).orElseThrow();
//        long start = System.currentTimeMillis();
//        var res = algo.getShortestPath(warehouse, dest);
//        long end = System.currentTimeMillis();
//        System.out.printf("Took %dms\n", end - start);
//        algo.printPath(res);

//        var dest = algo.getNode(0, 0).orElseThrow();
//        var res = algo.getShortestPath(warehouse, dest);
//        algo.printPath(res);

//        for (var dateStr: dates) {
//            System.out.printf("Date %s\n\n", dateStr);
//
//            var date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("dd/MM/yyyy")).atStartOfDay();
//            inputDataReader.setBlocksFromFile(date);
//            var orders = inputDataReader.getOrdersFromFileBenchmark(date);
//            for (int i = 0; i < 15; i++) {
//                long startTime = System.currentTimeMillis();
//                for (var order: orders) {
//                    for (var point: blocks) {
//                        algo.getNode(point).orElseThrow().setG(Integer.MAX_VALUE);
//                    }
//                    var node = algo.getNode(order.getDestination()).orElseThrow();
//                    algo.getShortestPath(warehouse, node);
//                }
//                long endTime = System.currentTimeMillis();
//                System.out.printf("%dms\n", endTime - startTime);
//            }
//            System.out.println();
//        }

//        System.out.println("Dijkstra algorithm");
//        var algo = new Dijkstra();
//        algo.initializeNodeMap(70, 50);
//        var warehouse = algo.getNode(45, 30).orElseThrow();
//        for (var dateStr: dates) {
//            System.out.printf("Date %s\n\n", dateStr);
//
//            var date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("dd/MM/yyyy")).atStartOfDay();
//            inputDataReader.setBlocksFromFile(date);
//            var orders = inputDataReader.getOrdersFromFileBenchmark(date);
//            for (int i = 0; i < 15; i++) {
//                long startTime = System.currentTimeMillis();
//                for (var order: orders) {
//                    for (var point: blocks) {
//                        algo.getNode(point).orElseThrow().setG(Integer.MAX_VALUE);
//                    }
//                    var node = algo.getNode(order.getX(), order.getY()).orElseThrow();
//                    algo.getShortestPath(warehouse, node);
//                }
//                long endTime = System.currentTimeMillis();
//                System.out.printf("%dms\n", endTime - startTime);
//            }
//            System.out.println();
//        }
    }
}
