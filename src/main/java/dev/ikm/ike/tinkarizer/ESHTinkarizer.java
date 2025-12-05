package dev.ikm.ike.tinkarizer;

import dev.ikm.ike.tinkarizer.database.Database;
import dev.ikm.ike.tinkarizer.entity.NavigableDatum;
import dev.ikm.ike.tinkarizer.entity.NavigableExtract;
import dev.ikm.ike.tinkarizer.entity.ViewableDatum;
import dev.ikm.ike.tinkarizer.entity.ViewableExtract;
import dev.ikm.ike.tinkarizer.etl.Extractor;
import dev.ikm.ike.tinkarizer.etl.Loader;
import dev.ikm.ike.tinkarizer.etl.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class ESHTinkarizer {

	private static final Logger LOG = LoggerFactory.getLogger(ESHTinkarizer.class);

	private final String databaseName;
	private final File dbPath;
	private final File eventSetCSV;
	private final File eventCodeCSV;

	public ESHTinkarizer(String databaseName, File dbPath, File eventSetCSV, File eventCodeCSV) {
		this.databaseName = databaseName;
		this.dbPath = dbPath;
		this.eventSetCSV = eventSetCSV;
		this.eventCodeCSV = eventCodeCSV;
	}

	public void run() {
		try (Database database = new Database(dbPath, databaseName)) {

			//Extract
			Extractor extractor = new Extractor();
			List<ViewableExtract> viewableExtracts = extractor.viewableData(eventSetCSV, eventCodeCSV);
			List<NavigableExtract> navigableExtracts = extractor.navigableData(eventSetCSV, eventCodeCSV);

			//Transform
			Transformer transformer = new Transformer();
			List<ViewableDatum> viewableData = transformer.transformViewableExtracts(viewableExtracts);
			List<NavigableDatum> navigableData = transformer.transformNavigableExtracts(navigableExtracts);

			//Load
			try (Loader loader = new Loader(System.currentTimeMillis())) {
				loader.loadViewableData(viewableData);
				loader.loadNavigableData(navigableData);
			}

			LOG.info("Skipped {} of Event Code to Event Set \"Is-As\" Relationships", transformer.getSkippedNavigableExtracts().size());
		}
	}
}
