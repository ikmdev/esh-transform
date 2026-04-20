package dev.ikm.ike.esh.tinkarizer.event.code;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.extract.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.index.EntityIndex;
import dev.ikm.ike.esh.tinkarizer.starter.data.ESHStarterData;

public class EventCodeExtractor implements Extractor {

	private List<File> eventCodeSources = new ArrayList<>();

	private final CSVFormat csvFormat;
	private final EntityIndex entityIndex;

	public EventCodeExtractor(EntityIndex cache) {
		this.entityIndex = cache;
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
	public List<NavigableSourceRecord> extractNavigableData() {
		// Event Code Navigable Data
		List<NavigableSourceRecord> ecNavigableData = new ArrayList<>();
		eventCodeSources.forEach(eventCodeSource -> {
			try (Reader ecReader = new FileReader(eventCodeSource); CSVParser ecParser = csvFormat.parse(ecReader)) {
				for (CSVRecord csvRecord : ecParser.getRecords()) {
					if (!csvRecord.get("Prev Display").isEmpty()) {
						NavigableSourceRecord navigableSourceRecord = new NavigableSourceRecord("active",
								csvRecord.get("Code Value"), csvRecord.get("Event Set Name"));
						ecNavigableData.add(navigableSourceRecord);
					}
				}
			} catch (IOException ioException) {
				throw new RuntimeException(ioException);
			}
		});
		return ecNavigableData;
	}

	@Override
	public List<ViewableSourceRecord> extractViewableData() {
		// Event Code Viewable Data
		List<ViewableSourceRecord> ecViewableData = new ArrayList<>();
		eventCodeSources.forEach(eventCodeSource -> {
			try (Reader ecReader = new FileReader(eventCodeSource); CSVParser ecParser = csvFormat.parse(ecReader)) {
				for (CSVRecord csvRecord : ecParser.getRecords()) {
					if (!csvRecord.get("Prev Display").isEmpty()) {
						ecViewableData.add(new ViewableSourceRecord(csvRecord.get("Code Value"),
								csvRecord.get("Status"), csvRecord.get("Prev Display"), csvRecord.get("Description"),
								csvRecord.get("Definition")));
						entityIndex.register(ESHStarterData.EVENT_CODE_NAMESPACE, csvRecord.get("Code Value"));
					}
				}
			} catch (IOException ioException) {
				throw new RuntimeException(ioException);
			}
		});
		return ecViewableData;
	}

}
