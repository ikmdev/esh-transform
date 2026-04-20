package dev.ikm.ike.esh.tinkarizer.event.set;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.extract.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.index.EntityIndex;
import dev.ikm.ike.esh.tinkarizer.starter.data.ESHStarterData;

public class EventSetExtractor implements Extractor {

	private List<File> eventSetSources = new ArrayList<>();

	private final CSVFormat csvFormat;
	private final EntityIndex entityIndex;

	public EventSetExtractor(EntityIndex entityIndex) {
		this.entityIndex = entityIndex;
		this.csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
	}

	@Override
	public void addFileToExtract(File file) {
		this.eventSetSources.add(file);
	}

	@Override
	public void addFilesToExtract(List<File> files) {
		files.forEach(this::addFileToExtract);
	}

	@Override
	public List<NavigableSourceRecord> extractNavigableData() {
		// Event Set Navigable Data
		List<NavigableSourceRecord> esNavigableData = new ArrayList<>();
		AtomicReference<String> parentNameReference = new AtomicReference<>();
		eventSetSources.forEach(eventSetSource -> {
			try (Reader esReader = new FileReader(eventSetSource); CSVParser esParser = csvFormat.parse(esReader)) {
				for (CSVRecord csvRecord : esParser.getRecords()) {
					if (!csvRecord.get("Event Set Name").isEmpty()) {
						parentNameReference.set(csvRecord.get("Event Set Name"));
						if (!csvRecord.get("Child Set Name").isEmpty()) {
							esNavigableData.add(new NavigableSourceRecord("active", csvRecord.get("Child Set Name"),
									parentNameReference.get()));
						}
					} else if (!csvRecord.get("Child Set Name").isEmpty()) {
						esNavigableData.add(new NavigableSourceRecord("active", csvRecord.get("Child Set Name"),
								parentNameReference.get()));
					}
				}
			} catch (IOException ioException) {
				throw new RuntimeException(ioException);
			}
		});
		return esNavigableData;
	}

	@Override
	public List<ViewableSourceRecord> extractViewableData() {
		// Event Set Viewable Data
		List<ViewableSourceRecord> esViewableData = new ArrayList<>();
		eventSetSources.forEach(eventSetSource -> {
			try (Reader esReader = new FileReader(eventSetSource); CSVParser esParser = csvFormat.parse(esReader)) {
				for (CSVRecord csvRecord : esParser.getRecords()) {
					if (!csvRecord.get("Event Set Name").isEmpty()) {
						esViewableData.add(new ViewableSourceRecord("", "", csvRecord.get("Event Set Name"),
								csvRecord.get("Event Set Disp"), csvRecord.get("Event Set Descr")));
						entityIndex.register(ESHStarterData.EVENT_SET_NAMESPACE, csvRecord.get("Event Set Name"));
					}
				}
			} catch (IOException ioException) {
				throw new RuntimeException(ioException);
			}
		});
		return esViewableData;
	}

}
