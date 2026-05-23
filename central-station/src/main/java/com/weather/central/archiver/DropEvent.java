package com.weather.central.archiver;

public class DropEvent {

    private final long stationId;
    private final long fromSeq;
    private final long toSeq;
    private final int droppedCount;
    private final long statusTimestamp;

    public DropEvent(long stationId, long fromSeq, long toSeq, int droppedCount, long statusTimestamp) {
        this.stationId = stationId;
        this.fromSeq = fromSeq;
        this.toSeq = toSeq;
        this.droppedCount = droppedCount;
        this.statusTimestamp = statusTimestamp;
    }

    public long getStationId() {
        return stationId;
    }

    public long getFromSeq() {
        return fromSeq;
    }

    public long getToSeq() {
        return toSeq;
    }

    public int getDroppedCount() {
        return droppedCount;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }
}

