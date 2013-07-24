package org.apache.hadoop.mapreduce;

public class Weights implements Comparable<Weights> {
        private final int[] weights;
        private final boolean allZeros;
        private final int isLinear;

        public Weights(int[] setWeights) {
            this.weights = new int[setWeights.length];
            boolean allZeros = true;
            for(int i = 0; i < weights.length; i++) {
                this.weights[i] = setWeights[i];
                if(this.weights[i] != 0) allZeros = false;
            }
            this.allZeros = allZeros;

            int isLinear = -1;
            for(int i = 0; i < weights.length; i++) {
                if(weights[i] > 1) {
                    isLinear = -1;
                    break;
                }
                if(weights[i] == 1) {
                    if(isLinear != -1) {
                        isLinear = -1;
                        break;
                    }
                    isLinear = i;
                }
            }
            this.isLinear = isLinear;
        }

        public int isLinear() {
            return this.isLinear;
        }

        public boolean allZeros() {
            return this.allZeros;
        }

        public int[] weights() {
            return this.weights;
        }

        @Override
        public String toString() {
            StringBuffer s = new StringBuffer();
            s.append("[");
            for(int i : weights) {
                s.append(" "+i);
            }
            s.append(" ]");
            return s.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Weights) {
                Weights other = (Weights)obj;
                if(other.weights.length == this.weights.length) {
                    for(int i = 0; i < weights.length; i++) {
                        if(this.weights[i] != other.weights[i]) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return weights[0] + weights[weights.length-1];
        }

        @Override
        public int compareTo(Weights other) {
            for(int i = 0; i < this.weights.length; i++) {
                if(this.weights[i] < other.weights[i]) {
                    return -1;
                } else if(this.weights[i] > other.weights[i]) {
                    return 1;
                }
            }
            return 0;
        }
    }
