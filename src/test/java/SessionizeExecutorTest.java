import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class SessionizeExecutorTest {

	private static SparkSession sparkSession;
	private static SessionizeExecutor executor;
	private static Dataset<Row> realData;

	@BeforeAll
    public static void setup() throws IOException {
		sparkSession = SparkSession.builder().appName("SessionizeExecutor").master("local[*]")
				.config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate();
		File testFile = new File(SessionizeExecutor.class.getClassLoader().getResource("test_file.log").getFile());
		BufferedReader brTest = new BufferedReader(new FileReader(testFile));
		executor = new SessionizeExecutor();
		String line = brTest.readLine();
		List<Row> rows = new ArrayList<>();
		while (StringUtils.isNotBlank(line)) {
			rows.add(executor.parseLine(line));
			line = brTest.readLine();
		}
		brTest.close();
		realData = sparkSession.createDataFrame(rows, executor.createAndGetSchema());
	}
	
	@AfterClass
	public static void afterClass() {
		if (sparkSession != null) {
			sparkSession.stop();
		}
	}
		
	private List<Row> generateTestRow() {
		List<Row> rows = new ArrayList<>();
		Row r1 = RowFactory.create("2015-07-22T10:35:47.920524Z", "marketpalce-shop", "1.38.21.23", "7673", "10.0.4.227", 
				"80", "0.000024", "0.027909", "0.000015", "302", "302", "0", "17", "GET https://paytm.com:443/shop/v1/frequentorders?channel=web&version=2 HTTP/1.1","Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"
				,"ECDHE-RSA-AES128-GCM-SHA256", "TLSv1.2");
		Row r2 = RowFactory.create("2015-07-22T10:55:47.920525Z", "marketpalce-shop", "1.38.21.20", "7673", "10.0.4.227", 
				"80", "0.000024", "0.027909", "0.000015", "302", "302", "0", "17", "GET https://paytm.com:443/shop/v1/frequentorders?channel=web&version=2 HTTP/1.1","Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"
				,"ECDHE-RSA-AES128-GCM-SHA256", "TLSv1.2");
		Row r3 = RowFactory.create("2015-07-22T10:59:47.920525Z", "marketpalce-shop", "1.38.21.20", "7673", "10.0.4.227", 
				"80", "0.000024", "0.027909", "0.000015", "302", "302", "0", "17", "GET https://paytm.com:443/shop/v1/frequentorders3?channel=web&version=2 HTTP/1.1","Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"
				,"ECDHE-RSA-AES128-GCM-SHA256", "TLSv1.2");
		Row r4 = RowFactory.create("2015-07-22T10:45:47.920525Z", "marketpalce-shop", "1.38.21.21", "7673", "10.0.4.227", 
				"80", "0.000024", "0.027909", "0.000015", "302", "302", "0", "17", "GET https://paytm.com:443/shop/v1/frequentorders2?channel=web&version=2 HTTP/1.1","Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"
				,"ECDHE-RSA-AES128-GCM-SHA256", "TLSv1.2");
		rows.add(r1);
		rows.add(r2);
		rows.add(r3);
		rows.add(r4);
		return rows;
	}
	
	@Test
	public void testParseLine() throws Exception {
		Dataset<Row> testData = sparkSession.createDataFrame(generateTestRow(), executor.createAndGetSchema());
		Assert.assertEquals(0, testData.except(realData).count());
	}
	
	@Test
	public void testAggregateSessionizeData() {
		Dataset<Row> aggregatedDataset = executor.aggregateSessionizeData(realData);
		Assert.assertEquals(2, aggregatedDataset.select("numberHitsInSessionForIp").where("clientIp='1.38.21.20'").first().getLong(0));
	}
	
	@Test
	public void testCalculateSessionDuration() {
		realData = realData.withColumn("timeStamp", realData.col("timeStamp").cast(DataTypes.TimestampType));
		Dataset<Row> aggregatedDataset = executor.calculateSessionDuration(realData, executor.aggregateSessionizeData(realData));
		aggregatedDataset.show(10, false);
		Assert.assertEquals(240, aggregatedDataset.agg(functions.max(aggregatedDataset.col("sessionDuration"))).first().getLong(0));
	}
}
