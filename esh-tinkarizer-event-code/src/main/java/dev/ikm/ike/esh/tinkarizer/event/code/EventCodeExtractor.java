package dev.ikm.ike.esh.tinkarizer.event.code;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import dev.ikm.ike.esh.tinkarizer.etl.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;

public class EventCodeExtractor implements Extractor {

	private List<File> eventCodeSources = new ArrayList<>();

	private final CSVFormat csvFormat;

	public EventCodeExtractor() {
		this.csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
	}

	@Override
	public void addFileToExtract(File file) {
		this.eventCodeSources.add(file);
	}

	@Override
	public void addFilesToExtract(List<File> files) {
		files.forEach(this::addFileToExtract);
	}

	@Override
	public List<NavigableSourceRecord> getExtractedNavigableData() {
		// Event Code Navigable Data
		List<NavigableSourceRecord> ecNavigableData = new ArrayList<>();
		eventCodeSources.forEach(eventCodeSource -> {
			try (Reader ecReader = new FileReader(eventCodeSource); CSVParser ecParser = csvFormat.parse(ecReader)) {
				for (CSVRecord csvRecord : ecParser.getRecords()) {
					if (!csvRecord.get("Prev Display").isEmpty()) {
						ecNavigableData
								.add(new NavigableSourceRecord(UUID.fromString("7291397d-9ead-4f1e-bcd2-3f8facdcf468"),
										csvRecord.get("Code Value"), csvRecord.get("Event Set Name")));
					}
				}
			} catch (IOException ioException) {
				throw new RuntimeException(ioException);
			}
		});
		return ecNavigableData;
	}

	@Override
	public List<ViewableSourceRecord> getExtractedViewableData() {
		// Event Code Viewable Data
		List<ViewableSourceRecord> ecViewableData = new ArrayList<>();
		eventCodeSources.forEach(eventCodeSource -> {
			try (Reader ecReader = new FileReader(eventCodeSource); CSVParser ecParser = csvFormat.parse(ecReader)) {
				for (CSVRecord csvRecord : ecParser.getRecords()) {
					if (!csvRecord.get("Prev Display").isEmpty()) {
						ecViewableData.add(new ViewableSourceRecord(
								UUID.fromString("7291397d-9ead-4f1e-bcd2-3f8facdcf468"), csvRecord.get("Code Value"),
								csvRecord.get("Status"), csvRecord.get("Prev Display"), csvRecord.get("Description"),
								csvRecord.get("Definition")));
					}
				}
			} catch (IOException ioException) {
				throw new RuntimeException(ioException);
			}
		});
		return ecViewableData;
	}
}
