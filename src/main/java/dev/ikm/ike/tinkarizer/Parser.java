package dev.ikm.ike.tinkarizer;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.terms.EntityProxy;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

class Parser {

	private static final Logger LOG = LoggerFactory.getLogger(Parser.class);
	private static final UUID ES_NAMESPACE = UUID.fromString("94282870-9279-46c0-b4ae-82cadddf7b7d");
	private static final UUID EC_NAMESPACE = UUID.fromString("7291397d-9ead-4f1e-bcd2-3f8facdcf468");

	private final int batchSize;
	private final File esCSV;
	private final File ecCSV;

	public Parser(int batchSize, File esCSV, File ecCSV) {
		this.batchSize = batchSize;
		this.esCSV = esCSV;
		this.ecCSV = ecCSV;
	}

	public void parseViewableData(Consumer<List<ViewableData>> viewableTransformationProcess) {
		LOG.info("Parsing Event Set Viewable Data...");
		Instant esViewableStart = Instant.now();
		processViewableData(
				esCSV,
				csvRecord -> csvRecord.get("Event Set Name").isEmpty(),
				csvRecord -> new ViewableData(
						ES_NAMESPACE,
						csvRecord.get("Event Set Name"),
						true,
						csvRecord.get("Event Set Name"),
						csvRecord.get("Event Set Disp"),
						csvRecord.get("Event Set Descr"),
						""),
				viewableTransformationProcess);
		Duration esVieableDuration = Duration.between(esViewableStart, Instant.now());
		LOG.info("Parsing Event Set Viewable Data complete in {} min {}s", esVieableDuration.toMinutes(), esVieableDuration.getSeconds() % 60);

		LOG.info("Parsing Event Code Viewable Data...");
		Instant ecViewableStart = Instant.now();
		processViewableData(
				ecCSV,
				csvRecord -> csvRecord.get("Prev Display").isEmpty(),
				csvRecord -> new ViewableData(
						EC_NAMESPACE,
						csvRecord.get("Code Value") + "|" + csvRecord.get("Prev Display"),
						csvRecord.get("Status").equalsIgnoreCase("active"),
						csvRecord.get("Prev Display"),
						csvRecord.get("Description"),
						csvRecord.get("Definition"),
						csvRecord.get("Code Value")),
				viewableTransformationProcess);
		Duration ecViewableDuration = Duration.between(ecViewableStart, Instant.now());
		LOG.info("Parsing Event Code Viewable Data complete in {} min {}s", ecViewableDuration.toMinutes(), ecViewableDuration.getSeconds() % 60);
	}

	public void parseNavigableData(Consumer<List<NavigableData>> navigableTransformationProcess) {

	}

	private void processViewableData(File csvFile,
									 Function<CSVRecord, Boolean> skipLogic,
									 Function<CSVRecord, ViewableData> mappingLogic,
									 Consumer<List<ViewableData>> transformation) {
		try (Reader reader = new FileReader(csvFile)) {
			CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
					.setHeader()
					.setSkipHeaderRecord(true)
					.get();
			ArrayList<ViewableData> batch = new ArrayList<>();
			for (CSVRecord csvRecord : csvFormat.parse(reader)) {
				if (batch.size() == batchSize) {
					transformation.accept(batch);
					batch.clear();
				} else {
					if (!skipLogic.apply(csvRecord)) {
						batch.add(mappingLogic.apply(csvRecord));
					}
				}
			}
			transformation.accept(batch);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void processNavigableData(File csvFile,
									   Function<CSVRecord, Boolean> skipLogic,
									   Function<CSVRecord, NavigableData> mappingLogic,
									  Consumer<List<NavigableData>> transformation) {

	}


}
