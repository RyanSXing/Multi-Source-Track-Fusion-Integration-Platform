package com.ryanxing.trackfusion.fusion;

import java.util.Arrays;

final class HungarianAssignment {
    private HungarianAssignment() {}

    static int[] solve(double[][] costs, double maxCost) {
        if (costs == null || !Double.isFinite(maxCost) || maxCost < 0) {
            throw new IllegalArgumentException("invalid assignment input");
        }
        int rows = costs.length;
        if (rows == 0) {
            return new int[0];
        }
        int columns = costs[0].length;
        for (double[] row : costs) {
            if (row == null || row.length != columns) {
                throw new IllegalArgumentException("cost matrix must be rectangular");
            }
        }
        int[] assignment = new int[rows];
        Arrays.fill(assignment, -1);
        if (columns == 0) {
            return assignment;
        }

        int size = Math.max(rows, columns);
        double unmatched = maxCost + 1;
        double forbidden = Math.max(1e12, unmatched * 1e6);
        double[][] square = new double[size][size];
        for (int row = 0; row < size; row++) {
            for (int column = 0; column < size; column++) {
                if (row >= rows) {
                    square[row][column] = 0;
                } else if (column >= columns) {
                    square[row][column] = unmatched;
                } else {
                    double cost = costs[row][column];
                    square[row][column] =
                            Double.isFinite(cost) && cost <= maxCost ? cost : forbidden;
                }
            }
        }

        double[] rowPotential = new double[size + 1];
        double[] columnPotential = new double[size + 1];
        int[] matchedRow = new int[size + 1];
        int[] previousColumn = new int[size + 1];
        for (int row = 1; row <= size; row++) {
            matchedRow[0] = row;
            double[] minimum = new double[size + 1];
            Arrays.fill(minimum, Double.POSITIVE_INFINITY);
            boolean[] used = new boolean[size + 1];
            int column = 0;
            do {
                used[column] = true;
                int currentRow = matchedRow[column];
                double delta = Double.POSITIVE_INFINITY;
                int nextColumn = 0;
                for (int candidate = 1; candidate <= size; candidate++) {
                    if (used[candidate]) {
                        continue;
                    }
                    double reduced =
                            square[currentRow - 1][candidate - 1]
                                    - rowPotential[currentRow]
                                    - columnPotential[candidate];
                    if (reduced < minimum[candidate]) {
                        minimum[candidate] = reduced;
                        previousColumn[candidate] = column;
                    }
                    if (minimum[candidate] < delta) {
                        delta = minimum[candidate];
                        nextColumn = candidate;
                    }
                }
                for (int candidate = 0; candidate <= size; candidate++) {
                    if (used[candidate]) {
                        rowPotential[matchedRow[candidate]] += delta;
                        columnPotential[candidate] -= delta;
                    } else {
                        minimum[candidate] -= delta;
                    }
                }
                column = nextColumn;
            } while (matchedRow[column] != 0);

            do {
                int previous = previousColumn[column];
                matchedRow[column] = matchedRow[previous];
                column = previous;
            } while (column != 0);
        }

        for (int column = 1; column <= size; column++) {
            int row = matchedRow[column] - 1;
            int originalColumn = column - 1;
            if (row >= 0
                    && row < rows
                    && originalColumn < columns
                    && Double.isFinite(costs[row][originalColumn])
                    && costs[row][originalColumn] <= maxCost) {
                assignment[row] = originalColumn;
            }
        }
        return assignment;
    }
}
