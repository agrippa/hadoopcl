package org.apache.hadoop.mapreduce;

public class HadoopCLSpaceAccounter {
    public enum SPACE_TYPE {
        FREE_INPUT_BUFFER, ALLOCED_INPUT_BUFFER,
        FREE_OUTPUT_BUFFER, ALLOCED_OUTPUT_BUFFER,
        INFLIGHT
    }

    private final SPACE_TYPE type;
    private final long space;

    public HadoopCLSpaceAccounter(SPACE_TYPE type, long space) {
        this.type = type;
        this.space = space;
    }

    private static String spaceTypeToString(SPACE_TYPE type) {
        switch(type) {
            case FREE_INPUT_BUFFER:
                return "free-input";
            case ALLOCED_INPUT_BUFFER:
                return "alloced-input";
            case FREE_OUTPUT_BUFFER:
                return "free-output";
            case ALLOCED_OUTPUT_BUFFER:
                return "alloced-output";
            case INFLIGHT:
                return "inflight";
        }
        return "invalid";
    }

    @Override
    public String toString() {
        return "[ "+spaceTypeToString(this.type)+"  "+this.space+" ]";
    }
}
