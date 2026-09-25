package br.com.ecommerce;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.Duration;
import java.time.Instant;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class EcommerceTrendJob {

    public static class EcommerceEvent {
        public String eventId;
        public String eventType;
        public String eventTime;
        public String productId;

        public EcommerceEvent() {
        }

        public EcommerceEvent(
                String eventId,
                String eventType,
                String eventTime,
                String productId) {

            this.eventId = eventId;
            this.eventType = eventType;
            this.eventTime = eventTime;
            this.productId = productId;
        }
    }

    public static class TrendResult {
        public String productId;
        public long count;
        public long windowStart;
        public long windowEnd;

        public TrendResult() {
        }

        public TrendResult(
                String productId,
                long count,
                long windowStart,
                long windowEnd) {

            this.productId = productId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return String.format(
                    "{\"product_id\":\"%s\",\"count\":%d,"
                    + "\"window_start\":\"%s\","
                    + "\"window_end\":\"%s\"}",
                    productId,
                    count,
                    Instant.ofEpochMilli(windowStart),
                    Instant.ofEpochMilli(windowEnd)
            );
        }
    }

    public static class JsonParser
            implements FlatMapFunction<String, EcommerceEvent> {

        @Override
        public void flatMap(
                String line,
                Collector<EcommerceEvent> out) {

            String eventId = value(line, "event_id");
            String eventType = value(line, "event_type");
            String eventTime = value(line, "event_time");
            String productId = value(line, "product_id");

            if (eventId == null
                    || eventType == null
                    || eventTime == null) {
                return;
            }

            out.collect(
                    new EcommerceEvent(
                            eventId,
                            eventType,
                            eventTime,
                            productId
                    )
            );
        }
    }

    public static class CountEvents
            implements AggregateFunction<
                    EcommerceEvent,
                    Long,
                    Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(
                EcommerceEvent event,
                Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class WindowResult
            extends ProcessWindowFunction<
                    Long,
                    TrendResult,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String productId,
                Context context,
                Iterable<Long> counts,
                Collector<TrendResult> out) {

            long count = counts.iterator().next();

            out.collect(
                    new TrendResult(
                            productId,
                            count,
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    private static String value(
            String json,
            String key) {

        String token = "\"" + key + "\":";

        int start = json.indexOf(token);

        if (start < 0) {
            return null;
        }

        start += token.length();

        while (start < json.length()
                && Character.isWhitespace(json.charAt(start))) {
            start++;
        }

        if (json.startsWith("null", start)) {
            return null;
        }

        if (start >= json.length()
                || json.charAt(start) != '"') {
            return null;
        }

        int end = json.indexOf('"', start + 1);

        if (end < 0) {
            return null;
        }

        return json.substring(start + 1, end);
    }

        public static class HBaseRestSink
                extends RichSinkFunction<TrendResult> {

        private final String baseUrl;
        private final String table;

        public HBaseRestSink(
                String baseUrl,
                String table) {

                this.baseUrl = baseUrl;
                this.table = table;
        }

        @Override
        public void invoke(
                TrendResult value,
                Context context) throws Exception {

                String rowKey =
                        value.productId + "-" + value.windowEnd;

                put(
                        rowKey,
                        "info:product_id",
                        value.productId
                );

                put(
                        rowKey,
                        "info:count",
                        String.valueOf(value.count)
                );

                put(
                        rowKey,
                        "info:window_start",
                        Instant.ofEpochMilli(
                                value.windowStart
                        ).toString()
                );

                put(
                        rowKey,
                        "info:window_end",
                        Instant.ofEpochMilli(
                                value.windowEnd
                        ).toString()
                );
        }

        private void put(
        String rowKey,
        String column,
        String value) throws Exception {

    String endpoint =
            baseUrl
                    + "/"
                    + table
                    + "/"
                    + rowKey
                    + "/"
                    + column;

    byte[] data =
            value.getBytes(
                    StandardCharsets.UTF_8
            );

    Exception lastError = null;

    for (int attempt = 1; attempt <= 3; attempt++) {

        HttpURLConnection connection = null;

        try {
            connection =
                    (HttpURLConnection)
                            new URL(endpoint)
                                    .openConnection();

            connection.setRequestMethod("PUT");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(20000);
            connection.setDoOutput(true);

            connection.setRequestProperty(
                    "Content-Type",
                    "application/octet-stream"
            );

            connection.setRequestProperty(
                    "Connection",
                    "close"
            );

            connection.setFixedLengthStreamingMode(
                    data.length
            );

            try (OutputStream output =
                         connection.getOutputStream()) {

                output.write(data);
                output.flush();
            }

            int responseCode =
                    connection.getResponseCode();

            if (responseCode >= 200
                    && responseCode < 300) {

                return;
            }

            throw new RuntimeException(
                    "HBase REST retornou HTTP "
                            + responseCode
                            + " para "
                            + endpoint
            );

        } catch (Exception error) {

            lastError = error;

            System.err.println(
                    "HBASE_RETRY tentativa="
                            + attempt
                            + " endpoint="
                            + endpoint
                            + " erro="
                            + error.getMessage()
            );

            if (attempt < 3) {
                Thread.sleep(1000L * attempt);
            }

        } finally {

            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    throw new RuntimeException(
            "Falha ao gravar no HBase REST apos 3 tentativas: "
                    + endpoint,
            lastError
    );
}

        
        }

    public static void main(String[] args)
            throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        env.setParallelism(1);

        FileSource<String> source =
                FileSource
                        .forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new Path("/stream/ready")
                        )
                        .monitorContinuously(
                                Duration.ofSeconds(2)
                        )
                        .build();

        DataStream<EcommerceEvent> events =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "flume-ready-files"
                        )
                        .flatMap(new JsonParser())
                        .name("parse-json");

        DataStream<EcommerceEvent> productEvents =
                events
                        .filter(event ->
                                event.productId != null
                                        && (
                                        event.eventType.equals("click")
                                        || event.eventType.equals("add_to_cart")
                                        || event.eventType.equals("purchase")
                                )
                        )
                        .name("eventos-de-produto");

        WatermarkStrategy<EcommerceEvent> watermarkStrategy =
                WatermarkStrategy
                        .<EcommerceEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5)
                        )
                        .withTimestampAssigner(
                                (event, timestamp) ->
                                        Instant.parse(
                                                event.eventTime
                                        ).toEpochMilli()
                        );

        DataStream<EcommerceEvent> timedEvents =
                productEvents
                        .assignTimestampsAndWatermarks(
                                watermarkStrategy
                        )
                        .name("event-time-watermarks");

        DataStream<TrendResult> trends =
                timedEvents
                        .keyBy(event -> event.productId)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.seconds(20),
                                        Time.seconds(5)
                                )
                        )
                        .aggregate(
                                new CountEvents(),
                                new WindowResult()
                        )
                        .name("tendencias-janela-deslizante");

trends.print("TREND");

DataStream<TrendResult> alerts =
        trends
                .filter(result ->
                        result.count >= 3
                )
                .name("alertas-tendencia");

alerts
        .addSink(
                new HBaseRestSink(
                        "http://hbase-rest:8080",
                        "trend_alerts"
                )
        )
        .name("hbase-rest-sink")
        .setParallelism(1);

        env.execute(
                "E-commerce - Tendencias em Tempo Real"
        );
    }
}