// LogAnalyticsEngine.java
// Multi-threaded Log Analytics Engine
// - Scans a folder of log files, parses lines, computes aggregations (counts, top messages, per-source stats)
// - Uses a thread pool and concurrent collectors
//
// Compile & run: javac LogAnalyticsEngine.java && java LogAnalyticsEngine <log-folder>
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.stream.*;

public class LogAnalyticsEngine {
    // simple parsed record
    static class Record {
        final Instant ts;
        final String level;
        final String source;
        final String message;
        Record(Instant ts, String level, String source, String message) {
            this.ts = ts; this.level = level; this.source = source; this.message = message;
        }
    }

    // parser for lines like: 2025-10-22T12:34:56Z [INFO] source - message
    static final Pattern LINE = Pattern.compile("^([\\d\\-T:\\.Z]+)\\s+\\[(\\w+)\\]\\s+(\\S+)\\s+-\\s+(.*)$");

    static Record parseLine(String line) {
        Matcher m = LINE.matcher(line);
        if (!m.matches()) return null;
        try {
            Instant ts = Instant.parse(m.group(1));
            return new Record(ts, m.group(2), m.group(3), m.group(4));
        } catch (Exception e) {
            return null;
        }
    }

    private final ExecutorService pool;
    private final Path folder;
    private final ConcurrentMap<String, LongAdder> levelCounts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LongAdder> sourceCounts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LongAdder> messageCounts = new ConcurrentHashMap<>();
    private final AtomicLong totalLines = new AtomicLong(0);

    public LogAnalyticsEngine(Path folder, int threads) {
        this.folder = folder;
        this.pool = Executors.newFixedThreadPool(threads);
    }

    public void ingestAll() throws IOException, InterruptedException {
        List<Path> files = Files.walk(folder).filter(Files::isRegularFile).collect(Collectors.toList());
        CountDownLatch latch = new CountDownLatch(files.size());
        for (Path f : files) {
            pool.submit(() -> {
                try (BufferedReader r = Files.newBufferedReader(f)) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        totalLines.incrementAndGet();
                        Record rec = parseLine(line);
                        if (rec != null) {
                            levelCounts.computeIfAbsent(rec.level, k->new LongAdder()).increment();
                            sourceCounts.computeIfAbsent(rec.source, k->new LongAdder()).increment();
                            String key = normalizeMessage(rec.message);
                            messageCounts.computeIfAbsent(key, k->new LongAdder()).increment();
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Failed reading " + f + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);
    }

    static String normalizeMessage(String msg) {
        // normalize numbers and ids to reduce cardinality
        return msg.replaceAll("\\b\\d+\\b", "<NUM>").replaceAll("/[A-Fa-f0-9]{8,}/", "<HEX>");
    }

    public void reportTop(int k) {
        System.out.println("Total lines: " + totalLines.get());
        System.out.println("Levels:");
        levelCounts.forEach((k1,v)->System.out.println("  " + k1 + " -> " + v.longValue()));
        System.out.println("Top sources:");
        sourceCounts.entrySet().stream()
            .sorted((a,b)->Long.compare(b.getValue().longValue(), a.getValue().longValue()))
            .limit(k).forEach(e->System.out.println("  " + e.getKey() + " -> " + e.getValue()));
        System.out.println("Top messages:");
        messageCounts.entrySet().stream()
            .sorted((a,b)->Long.compare(b.getValue().longValue(), a.getValue().longValue()))
            .limit(k).forEach(e->System.out.println("  " + e.getKey() + " -> " + e.getValue()));
    }

    // demo: create some synthetic logs if folder empty
    static void createSyntheticLogs(Path folder) throws IOException {
        Files.createDirectories(folder);
        Random rnd = new Random();
        String[] levels = {"INFO","WARN","ERROR","DEBUG"};
        String[] sources = {"auth","api","db","scheduler"};
        for (int f = 0; f < 5; f++) {
            Path p = folder.resolve("log" + f + ".log");
            try (BufferedWriter w = Files.newBufferedWriter(p)) {
                for (int i = 0; i < 2000; i++) {
                    Instant ts = Instant.now().minusSeconds(rnd.nextInt(3600));
                    String level = levels[rnd.nextInt(levels.length)];
                    String src = sources[rnd.nextInt(sources.length)];
                    String msg = switch (rnd.nextInt(5)) {
                        case 0 -> "User login success id=" + rnd.nextInt(1000);
                        case 1 -> "User login failed id=" + rnd.nextInt(1000);
                        case 2 -> "Query executed time_ms=" + rnd.nextInt(200);
                        case 3 -> "Scheduled job executed";
                        default -> "Connection to host 10.0." + rnd.nextInt(255) + "." + rnd.nextInt(255);
                    };
                    w.write(ts.toString() + " [" + level + "] " + src + " - " + msg + "\n");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Path folder = args.length > 0 ? Paths.get(args[0]) : Paths.get("logs");
        if (!Files.exists(folder) || Files.list(folder).findAny().isEmpty()) {
            System.out.println("No logs found; generating synthetic logs in " + folder);
            createSyntheticLogs(folder);
        }
        LogAnalyticsEngine engine = new LogAnalyticsEngine(folder, Math.max(2, Runtime.getRuntime().availableProcessors()));
        long t0 = System.nanoTime();
        engine.ingestAll();
        long t1 = System.nanoTime();
        System.out.println("Ingest time ms: " + ((t1 - t0) / 1_000_000));
        engine.reportTop(10);
    }
}
