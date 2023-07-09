package com.trackerhub.algorithms;

import java.awt.*;
import java.awt.geom.Point2D;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AStar {
    protected Map<Point, Node> nodeMap;

    public AStar() {}

    public AStar(int maxX, int maxY) {
        initializeNodeMap(maxX, maxY);
    }

    public void initializeNodeMap(int maxX, int maxY) {
        nodeMap = new ConcurrentHashMap<>();
        for (int i = 0; i <= maxX; i++) {
            for (int j = 0; j <= maxY; j++) {
                var point = new Point(i, j);
                var node = new Node(point);
                nodeMap.put(point, node);
            }
        }
        for (int i = 0; i <= maxX; i++) {
            for (int j = 0; j <= maxY; j++) {
                var cur = getNode(i, j).orElseThrow();
                var left = getNode(i - 1, j);
                var right = getNode(i + 1, j);
                var top = getNode(i, j + 1);
                var bottom = getNode(i, j - 1);

                left.ifPresent(cur::addNeighbor);
                right.ifPresent(cur::addNeighbor);
                top.ifPresent(cur::addNeighbor);
                bottom.ifPresent(cur::addNeighbor);
            }
        }
    }

    public Node getShortestPath(Node start, Node target) {
        var closedList = new PriorityQueue<Node>();
        var openList = new PriorityQueue<Node>();

        start.setF(start.getG() + calculateHeuristic(start, target));

        var visited = new HashSet<Node>();
        openList.add(start);
        Node result = null;

        while (!openList.isEmpty()) {
            var n = openList.peek();
            if (n == target) {
                result = n;
                break;
            }

            for (var m: n.getNeighbors()) {
                if (visited.contains(m)) continue;
                long totalWeight = n.getG() + m.getG();
                if (m.getG() >= Integer.MAX_VALUE) {
                    if (m == target) {
                        m.setParent(n);
                        return m;
                    }
                    continue;
                }

                if (!openList.contains(m) && !closedList.contains(m)) {
                    m.setParent(n);
                    m.setG(totalWeight);
                    m.setF(m.getG() + calculateHeuristic(m, target));
                    openList.add(m);
                } else {
                    if (n.getG() < m.getG()) {
                        m.setParent(n);
                        m.setG(n.getG() + 1);
                        m.setF(m.getG() + calculateHeuristic(m, target));

                        if (closedList.contains(m)) {
                            closedList.remove(m);
                            openList.add(m);
                        }
                    }
                }
            }

            visited.add(n);
            openList.remove(n);
            closedList.add(n);
        }

        return result;
    }

    public void cleanNodes() {
        nodeMap.forEach((k, v) -> {
            v.setG(1);
            v.setParent(null);
        });
    }

    public void printPath(Node node) {
        var runner = node;

        if (runner == null) return;

        var nodes = new ArrayList<Node>();
        long totalNodes = 1;

        while (runner.getParent() != null) {
            nodes.add(runner);
            runner = runner.getParent();
            totalNodes++;
        }
        nodes.add(runner);
        Collections.reverse(nodes);

        for (var n: nodes) {
            System.out.printf("(%d, %d)\n", n.getPoint().x, n.getPoint().y);
        }
        System.out.printf("Total nodes traversed: %d\n", totalNodes);
    }

    public List<Node> getShortestPathList(Node start, Node target) {
        var node = getShortestPath(start, target);
        var res = new ArrayList<Node>();
        var runner = node;

        while (runner.getParent() != null) {
            res.add(runner);
            runner = runner.getParent();
        }
        res.add(runner);
        Collections.reverse(res);
        return res;
    }

    protected long calculateHeuristic(Node cur, Node target) {
        int D = 1;
        int dx = Math.abs(cur.getPoint().x - target.getPoint().x);
        int dy = Math.abs(cur.getPoint().y - target.getPoint().y);
        return D * (dx + dy);
    }

    public Optional<Node> getNode(int x, int y) {
        return Optional.ofNullable(nodeMap.get(new Point(x, y)));
    }

    public Optional<Node> getNode(Point2D.Double point) {
        return getNode((int)point.x, (int)point.y);
    }

    public Optional<Node> getNode(Point point) {
        return Optional.ofNullable(nodeMap.get(point));
    }
}
