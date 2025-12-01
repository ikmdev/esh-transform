package dev.ikm.ike;

import dev.ikm.ike.tinkarizer.ESHTinkarizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;

public class App {

	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	private static final Instant start = Instant.now();

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			LOG.error("Usage: java App <db_path> <event_set_csv_path> <event_code_csv_path>");
			System.exit(1);
		}

		ESHTinkarizer eshtinkarizer = new ESHTinkarizer(
				"Open SpinedArrayStore",
				new File(args[0]),
				new File(args[1]),
				new File(args[2]));
		eshtinkarizer.run();


		Duration duration = Duration.between(start, Instant.now());
		LOG.info("Transformation complete in {} min {}s", duration.toMinutes(), duration.getSeconds() % 60);
	}
}
