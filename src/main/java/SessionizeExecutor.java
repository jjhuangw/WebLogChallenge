import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/**
 * Web Log SessionizeExecutor
 *
 * @author chienchang.huang
 * 
 */
public class SessionizeExecutor implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Pattern LOG_PATTERN = Pattern.compile(
			"^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"(\\S+ \\S+ \\S+)\" \"([^\"]*)\" (\\S+) (\\S+)");

	private static final String SESSION_WINDOW_SIZE = "15 minutes";

	private static final String COLUMN_CLIENT_IP = "clientIp";
	private static final String COLUMN_FIXED_SESSION_WINDOW = "fixedSessionWindow";
	private static final String COLUMN_SESSION_DURATION = "sessionDuration";
	private static final String COLUMN_TIME_STAMP = "timeStamp";

	// Log format please refer to
	// https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
	private StructType createAndGetSchema() {
		List<StructField> structFields = new ArrayList<>();
		structFields.add(DataTypes.createStructField(COLUMN_TIME_STAMP, DataTypes.StringType, false));
		structFields.add(DataTypes.createStructField("elb", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField(COLUMN_CLIENT_IP, DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("clientPort", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("backendIp", DataTypes.StringType, false));
		structFields.add(DataTypes.createStructField("backendPort", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("requestProcessingTimeInSecond", DataTypes.StringType, false));
		structFields.add(DataTypes.createStructField("backendProcessingTimeInSecond", DataTypes.StringType, false));
		structFields.add(DataTypes.createStructField("responseProcessingTime", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("elbStatusCode", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("backendStatusCode", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("receivedBytes", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sentBytes", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("request", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("userAgent", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sslCipher", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sslProtocol", DataTypes.StringType, true));
		return DataTypes.createStructType(structFields);
	}

	/**
	 * Parse line and transform to structure
	 *
	 * @param line
	 * @return Row structure
	 */
	private Row parseLine(String line) {
		Matcher matcher = LOG_PATTERN.matcher(line);
		matcher.find();

		String[] clientIpPort = { "-", "-" };
		String[] backendIpPort = { "-", "-" };
		if (!matcher.group(3).equalsIgnoreCase("-")) {
			clientIpPort = matcher.group(3).split(":");
		}
		if (!matcher.group(4).equalsIgnoreCase("-")) {
			backendIpPort = matcher.group(4).split(":");
		}
		return RowFactory.create(matcher.group(1), matcher.group(2), clientIpPort[0], clientIpPort[1], backendIpPort[0],
				backendIpPort[1], matcher.group(5), matcher.group(6), matcher.group(7), matcher.group(8),
				matcher.group(9), matcher.group(10), matcher.group(11), matcher.group(12), matcher.group(13),
				matcher.group(14), matcher.group(15));
	}

	/**
	 * Aggregate all page hits by visitor/IP during a session.
	 *
	 * @param main data set
	 * @return sessionized data set
	 */
	private Dataset<Row> aggregateSessionizeData(Dataset<Row> mainDataset) {
		Dataset<Row> sessionDataset = mainDataset
				.select(functions.window(mainDataset.col(COLUMN_TIME_STAMP), SESSION_WINDOW_SIZE)
						.as(COLUMN_FIXED_SESSION_WINDOW), mainDataset.col(COLUMN_TIME_STAMP),
						mainDataset.col(COLUMN_CLIENT_IP))
				.groupBy(COLUMN_FIXED_SESSION_WINDOW, COLUMN_CLIENT_IP).count()
				.withColumnRenamed("count", "numberHitsInSessionForIp");
		return sessionDataset.withColumn("sessionId", functions.monotonically_increasing_id());
	}

	/**
	 * Find the session duration (between the first hit and last hit)
	 *
	 * @param mainDataset
	 * @param sessionizedDataset
	 * @return sessionizedDataset with session duration
	 */
	private Dataset<Row> calculateSessionDuration(Dataset<Row> mainDataset, Dataset<Row> sessionizeDataset) {
		Dataset<Row> datasetWithTimeStamp = mainDataset.select(
				functions.window(mainDataset.col(COLUMN_TIME_STAMP), SESSION_WINDOW_SIZE)
						.alias(COLUMN_FIXED_SESSION_WINDOW),
				mainDataset.col(COLUMN_TIME_STAMP), mainDataset.col(COLUMN_CLIENT_IP), mainDataset.col("request"));

		Seq<String> joinColumns = JavaConverters
				.asScalaBuffer(Arrays.asList(COLUMN_FIXED_SESSION_WINDOW, COLUMN_CLIENT_IP)).toList();

		Dataset<Row> sessionizeWithDurationDataset = datasetWithTimeStamp.join(sessionizeDataset, joinColumns);

		Dataset<Row> firstHitTimeStamps = sessionizeWithDurationDataset.groupBy("sessionId")
				.agg(functions.min(COLUMN_TIME_STAMP).alias("firstHitTimeStamp"));

		sessionizeWithDurationDataset = firstHitTimeStamps.join(sessionizeWithDurationDataset, "sessionId");

		sessionizeWithDurationDataset = sessionizeWithDurationDataset.withColumn("timeDiffBetweenFirstHit",
				functions.unix_timestamp(sessionizeWithDurationDataset.col(COLUMN_TIME_STAMP))
						.minus(functions.unix_timestamp(sessionizeWithDurationDataset.col("firstHitTimeStamp"))));

		// find the session duration
		Dataset<Row> sessionDurationDf = sessionizeWithDurationDataset.groupBy("sessionId")
				.agg(functions.max("timeDiffBetweenFirstHit").alias(COLUMN_SESSION_DURATION));

		return sessionizeWithDurationDataset.join(sessionDurationDf, "sessionId");
	}

	private Dataset<Row> calculateAverageSessionDuration(Dataset<Row> sessionizeWithDurationDataset) {
		return sessionizeWithDurationDataset
				.select(functions.avg(COLUMN_SESSION_DURATION).alias("averageSessionDuration"));
	}

	private Dataset<Row> findUniqueURLPerSession(Dataset<Row> sessionizeDataset) {
		return sessionizeDataset.groupBy("sessionId", "request").count().distinct().withColumnRenamed("count",
				"uniqueUrlCount");
	}

	private Dataset<Row> findMostEngagedUsers(Dataset<Row> sessionizeWithDurationDataset) {
		return sessionizeWithDurationDataset.select(COLUMN_CLIENT_IP, "sessionId", COLUMN_SESSION_DURATION)
				.sort(sessionizeWithDurationDataset.col(COLUMN_SESSION_DURATION).desc()).distinct();
	}

	public void execute(String filePath) {
		// TODO: local mode only for development, before release remember change to cluster mode
		SparkSession sparkSession = SparkSession.builder().appName("SessionizeExecutor").master("local[*]")
				.config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate();

		// data manipulation and data clean
		Dataset<Row> parsedDataset = sparkSession.read().textFile(filePath)
				.filter((FilterFunction<String>) r -> LOG_PATTERN.matcher((String) r).find())
				.map((MapFunction<String, Row>) line -> parseLine(line), RowEncoder.apply(createAndGetSchema()));
		Dataset<Row> mainDataset = parsedDataset.filter(
				"backendIp != '-' AND requestProcessingTimeInSecond !='-1' AND backendProcessingTimeInSecond !='-1' AND responseProcessingTime !='-1' ")
				.withColumn(COLUMN_TIME_STAMP, parsedDataset.col(COLUMN_TIME_STAMP).cast(DataTypes.TimestampType));
		mainDataset.cache();

		// Sessionize web log by IP. Aggregate all page hits by visitor/IP during a
		// session.
		Dataset<Row> sessionizeDataset = aggregateSessionizeData(mainDataset);

		// Calculate session duration
		Dataset<Row> sessionizeWithDurationDataset = calculateSessionDuration(mainDataset, sessionizeDataset);
		sessionizeWithDurationDataset.cache();

		// TODO: Calculate and output result. Here, csv and repartition is for debugging purpose.
		// Determine the average session time
		Dataset<Row> averageSessionDurationsDataset = calculateAverageSessionDuration(sessionizeWithDurationDataset);
		averageSessionDurationsDataset.repartition(1).write().option("sep", ";").option("header", "true")
				.mode("overwrite").csv("./result/averageSessionDuration");

		// Determine unique URL visits per session. To clarify, count a hit to a unique
		// URL only once per session.
		Dataset<Row> uniqueVisitDataset = findUniqueURLPerSession(sessionizeWithDurationDataset);
		uniqueVisitDataset.repartition(1).write().option("sep", ";").option("header", "true").mode("overwrite")
				.csv("./result/uniqueVisit");

		// Find the most engaged users, ie the IPs with the longest session times
		Dataset<Row> mostEngagedDataSet = findMostEngagedUsers(sessionizeWithDurationDataset);
		mostEngagedDataSet.repartition(1).write().option("sep", ";").option("header", "true").mode("overwrite")
				.csv("./result/mostEngaged");

	}

	public static void main(String[] args) {
		File file = new File(SessionizeExecutor.class.getClassLoader()
				.getResource("2015_07_22_mktplace_shop_web_log_sample.log").getFile());

		SessionizeExecutor executor = new SessionizeExecutor();
		executor.execute(file.getAbsolutePath());
	}

}
