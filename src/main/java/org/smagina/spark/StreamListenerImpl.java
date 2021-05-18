package org.smagina.spark;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.round;

public class StreamListenerImpl extends StreamingQueryListener {
    private StreamingQuery sq;

    public StreamListenerImpl(StreamingQuery sq) {
        this.sq = sq;
    }

    @Override
    public void onQueryStarted(QueryStartedEvent e) {
        System.out.println(String.format("Stream started: id=%s runId=%s", e.id()));
    }

    @Override
    public void onQueryProgress(QueryProgressEvent e) {
        StreamingQueryProgress p = e.progress();
        boolean isDataAvailable = sq.status().isDataAvailable();
        System.out.println(String.format("Stream progress: id=%s batchId=%s isDataAvailable=%s rows=%s velocity=%srps", p.id(), p.batchId(), isDataAvailable, p.numInputRows(), round(p.processedRowsPerSecond())));
        if (!isDataAvailable) {
            System.out.println("Stop streaming due to no data available");
            sq.stop();
        }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent e) {
        System.out.println(String.format("Stream Finished: id=%s", e.id()));
    }
}
