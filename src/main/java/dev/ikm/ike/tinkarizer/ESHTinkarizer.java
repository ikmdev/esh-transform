package dev.ikm.ike.tinkarizer;

import dev.ikm.ike.tinkarizer.database.Database;
import dev.ikm.ike.tinkarizer.entity.NavigableData;
import dev.ikm.ike.tinkarizer.entity.ViewableData;
import dev.ikm.ike.tinkarizer.extract.Extractor;
import dev.ikm.ike.tinkarizer.tranform.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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

	public void run() throws Exception {
		try (Database database = new Database(dbPath, databaseName)) {
			try (Transformer transformer = new Transformer(System.currentTimeMillis())) {
				try (Extractor extractor = new Extractor(eventSetCSV, eventCodeCSV, 50_000)) {
					List<CompletableFuture<Void>> completableFutures = new ArrayList<>();

					for (List<ViewableData> viewableData : extractor.viewableData()) {
						completableFutures.add(CompletableFuture.runAsync(() -> {
							transformer.viewableTransformation(viewableData);
						}));
					}

					for (List<NavigableData> navigableData : extractor.navigableData()) {
						completableFutures.add(CompletableFuture.runAsync(() -> {
							transformer.navigableTransformation(navigableData);
						}));
					}
					CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).join();
				}
			}
		}
		LOG.info("Skipped {} of Event Code to Event Set Is-As", (long) Debug.ecParents.size());
	}
}
