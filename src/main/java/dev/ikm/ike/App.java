package dev.ikm.ike;

import dev.ikm.ike.tinkarizer.ESHTinkarizer;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class App {

	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	private static final Instant start = Instant.now();

	public static void main(String[] args) {
		if (args.length < 3) {
			LOG.error("Usage: java App <db_path> <event_set_csv_path> <event_code_csv_path>");
			System.exit(1);
		}

		ESHTinkarizer eshtinkarizer = new ESHTinkarizer(
				"Open SpinedArrayStore",
				new File(args[0]),
				new File(args[1]),
				new File(args[2]),
				System.currentTimeMillis());
		eshtinkarizer.run();


		Duration duration = Duration.between(start, Instant.now());
		LOG.info("Transformation complete in {} min {}s", duration.toMinutes(), duration.getSeconds() % 60);
	}

	private void flushESBatch(List<CSVRecord> csvRecords) {
//		LOG.info("Flushing ES Batch of {}", csvRecords.size());
//		AtomicReference<CSVRecord> parent = new AtomicReference<>();
//		csvRecords.forEach(csvRecord -> {
//			if (!csvRecord.get("Event Set Name").isEmpty()) {
//				parent.set(csvRecord);
//				System.out.println("Parent: " + csvRecord.get("Event Set Name"));
//				viewableTransformation(
//						ES_NAMESPACE,
//						csvRecord.get("Event Set Name"),
//						true,
//						csvRecord.get("Event Set Name"),
//						csvRecord.get("Event Set Disp"),
//						csvRecord.get("Event Set Descr")
//				);
//				if (!csvRecord.get("Child Set Name").isEmpty()) {
//					navigableTransformation(
//							ES_NAMESPACE,
//							csvRecord.get("Child Set Name"),
//							ES_NAMESPACE,
//							csvRecord.get("Event Set Name")
//					);
//					System.out.println("\t" + csvRecord.get("Child Set Name"));
//				}
//			} else {
//				if (parent.get() == null) {
//					System.out.println("break");
//				}
//				System.out.println("\t" + parent.get().get("Event Set Name"));
//				navigableTransformation(
//						ES_NAMESPACE,
//						csvRecord.get("Child Set Name"),
//						ES_NAMESPACE,
//						parent.get().get("Event Set Name")
//				);
//			}
//		});
	}

//	private Runnable eventCodeTransformation(String ecCSV) {
//		return () -> {
//			try (Reader reader = new FileReader(ecCSV)) {
//				CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
//						.setHeader()
//						.setSkipHeaderRecord(true)
//						.get();
//				for (CSVRecord record : csvFormat.parse(reader)) {
//					if (!record.get("Prev Display").isEmpty()) {
//						viewableTransformation(
//								EC_NAMESPACE,
//								record.get("Code Value") + "|" + record.get("Prev Display"),
//								record.get("Status").equals("Active"),
//								record.get("Prev Display"),
//								record.get("Description"),
//								record.get("Definition")
//						);
//					}
//				}
//			} catch (FileNotFoundException e) {
//				LOG.error("Event code file not found: {}", ecCSV, e);
//			} catch (IOException e) {
//				LOG.error("Error reading event code file: {}", ecCSV, e);
//			}
//		};
//	}

}
