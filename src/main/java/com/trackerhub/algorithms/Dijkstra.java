package com.trackerhub.algorithms;

public class Dijkstra extends AStar {
    @Override
    protected long calculateHeuristic(Node cur, Node target) {
        return 0;
    }
}
