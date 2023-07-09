package com.trackerhub.algorithms;

import lombok.Getter;
import lombok.Setter;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
public class Node implements Comparable<Node> {
    private static int idCounter = 0;
    private UUID uuid = UUID.randomUUID();
    private Point point;
    private Node parent;
    private List<Node> neighbors = new ArrayList<>();
    private long f = 1;
    private long g = 1;

    public Node(Point point) {
        this.point = point;
    }

    @Override
    public int compareTo(Node node) {
        return Long.compare(getF(), node.getF());
    }

    public void addNeighbor(Node node) {
        neighbors.add(node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return uuid.equals(node.uuid);
    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }
}
