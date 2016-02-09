package benchmark;

/**
 * Benchmark class for running Logstash
 */
public class LogstashBenchmark extends Benchmark {

    private static final String LOGSTASH_CONFIGURATION = "/logstash.conf";
    private static final String LOGSTASH_CONFIGURATION_FLAG = "-f";

    private final String pathToLogstash;
    private final String pathToConfiguration;

    public LogstashBenchmark(String pathToLogstash) {
        this.pathToLogstash = pathToLogstash;
        pathToConfiguration = getClass().getResource(LOGSTASH_CONFIGURATION).getPath();
    }

    @Override
    protected Process getProcess() throws Exception {
        ProcessBuilder builder = new ProcessBuilder()
                .command(pathToLogstash, LOGSTASH_CONFIGURATION_FLAG, pathToConfiguration);
        return builder.start();
    }

}
