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
import dev.ikm.ike.esh.tinkarizer.etl.extractor.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.index.EntityIndex;

public class EventSetExtractor implements Extractor {

	private File esCSV;
	private File ecCSV;

	private final CSVFormat csvFormat;
	private final EntityIndex cache;

	public EventSetExtractor(EntityIndex cache) {
		this.cache = cache;
		this.csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
	}

	@Override
	public void addFileToExtract(File file) {
		if (file.getName().contains("event_set")) {
			this.esCSV = file;
		} else if (file.getName().contains("event_code")) {
			this.ecCSV = file;
		} else {
			throw new IllegalArgumentException("Unexpected file: " + file.getName());
		}
	}

	@Override
	public void addFilesToExtract(List<File> files) {
		files.forEach(this::addFileToExtract);
	}

	@Override
	public List<NavigableSourceRecord> getExtractedNavigableData() {
		// Event Set Navigable Data
		List<NavigableSourceRecord> esNavigableData = new ArrayList<>();
		AtomicReference<String> parentNameReference = new AtomicReference<>();

		try (Reader esReader = new FileReader(esCSV); CSVParser esParser = csvFormat.parse(esReader)) {
			for (CSVRecord csvRecord : esParser.getRecords()) {
				if (!csvRecord.get("Event Set Name").isEmpty()) {
					parentNameReference.set(csvRecord.get("Event Set Name"));
					if (!csvRecord.get("Child Set Name").isEmpty()) {
						esNavigableData.add(
								new NavigableSourceRecord(csvRecord.get("Child Set Name"), parentNameReference.get()));
					}
				} else if (!csvRecord.get("Child Set Name").isEmpty()) {
					esNavigableData
							.add(new NavigableSourceRecord(csvRecord.get("Child Set Name"), parentNameReference.get()));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}

		// Event Code Navigable Data
		List<NavigableSourceRecord> ecNavigableData = new ArrayList<>();
		try (Reader ecReader = new FileReader(ecCSV); CSVParser ecParser = csvFormat.parse(ecReader)) {
			for (CSVRecord csvRecord : ecParser.getRecords()) {
				if (!csvRecord.get("Prev Display").isEmpty()) {
					ecNavigableData.add(
							new NavigableSourceRecord(csvRecord.get("Code Value"), csvRecord.get("Event Set Name")));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}

		// Combine both Event Set and Event Code Navigable Data
		List<NavigableSourceRecord> allNavigableData = new ArrayList<>();
		allNavigableData.addAll(esNavigableData);
		allNavigableData.addAll(ecNavigableData);
		return allNavigableData;
	}

	@Override
	public List<ViewableSourceRecord> getExtractedViewableData() {
		// Event Set Viewable Data
		List<ViewableSourceRecord> esViewableData = new ArrayList<>();
		try (Reader esReader = new FileReader(esCSV); CSVParser esParser = csvFormat.parse(esReader)) {
			for (CSVRecord csvRecord : esParser.getRecords()) {
				if (!csvRecord.get("Event Set Name").isEmpty()) {
					esViewableData.add(new ViewableSourceRecord("", "", csvRecord.get("Event Set Name"),
							csvRecord.get("Event Set Disp"), csvRecord.get("Event Set Descr")));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}

		// Event Code Viewable Data
		List<ViewableSourceRecord> ecViewableData = new ArrayList<>();
		try (Reader ecReader = new FileReader(ecCSV); CSVParser ecParser = csvFormat.parse(ecReader)) {
			for (CSVRecord csvRecord : ecParser.getRecords()) {
				if (!csvRecord.get("Prev Display").isEmpty()) {
					ecViewableData.add(new ViewableSourceRecord(csvRecord.get("Code Value"), csvRecord.get("Status"),
							csvRecord.get("Prev Display"), csvRecord.get("Description"), csvRecord.get("Definition")));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}

		// Combine both Event Set and Event Code Viewable Data
		List<ViewableSourceRecord> allViewableData = new ArrayList<>();
		allViewableData.addAll(esViewableData);
		allViewableData.addAll(ecViewableData);
		return allViewableData;
	}
}
