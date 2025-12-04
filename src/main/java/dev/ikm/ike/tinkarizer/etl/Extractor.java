package dev.ikm.ike.tinkarizer.etl;

import dev.ikm.ike.tinkarizer.entity.NavigableExtract;
import dev.ikm.ike.tinkarizer.entity.Type;
import dev.ikm.ike.tinkarizer.entity.ViewableExtract;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class Extractor{

	private final CSVFormat csvFormat;

	public Extractor() {
		this.csvFormat = CSVFormat.DEFAULT.builder()
				.setHeader()
				.setSkipHeaderRecord(true)
				.get();
	}

	public List<ViewableExtract> viewableData(File esCSV, File ecCSV) {
		return Stream.concat(esViewableData(esCSV).stream(), ecViewableData(ecCSV).stream()).toList();
	}

	private List<ViewableExtract> esViewableData(File esCSV) {
		List<ViewableExtract> esViewableData = new ArrayList<>();
		try (Reader esReader = new FileReader(esCSV);
			 CSVParser esParser = csvFormat.parse(esReader)) {
			for (CSVRecord csvRecord : esParser.getRecords()) {
				if (!csvRecord.get("Event Set Name").isEmpty()) {
					esViewableData.add(new ViewableExtract(
							Type.EVENT_SET,
							"",
							"",
							csvRecord.get("Event Set Name"),
							csvRecord.get("Event Set Disp"),
							csvRecord.get("Event Set Descr")));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}
		return esViewableData;
	}

	private List<ViewableExtract> ecViewableData(File ecCSV) {
		List<ViewableExtract> ecViewableData = new ArrayList<>();
		try (Reader ecReader = new FileReader(ecCSV);
			 CSVParser ecParser = csvFormat.parse(ecReader)) {
			for (CSVRecord csvRecord : ecParser.getRecords()) {
				if (!csvRecord.get("Prev Display").isEmpty()) {
					ecViewableData.add(new ViewableExtract(
							Type.EVENT_CODE,
							csvRecord.get("Code Value"),
							csvRecord.get("Status"),
							csvRecord.get("Prev Display"),
							csvRecord.get("Description"),
							csvRecord.get("Definition")));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}
		return ecViewableData;
	}

	public List<NavigableExtract> navigableData(File esCSV, File ecCSV) {
		return Stream.concat(esNavigableData(esCSV).stream(), ecNavigableData(ecCSV).stream()).toList();
	}

	private List<NavigableExtract> esNavigableData(File esCSV) {
		List<NavigableExtract> esNavigableData = new ArrayList<>();
		AtomicReference<String> parentNameReference = new AtomicReference<>();

		try (Reader esReader = new FileReader(esCSV);
			 CSVParser esParser = csvFormat.parse(esReader)) {
			for (CSVRecord csvRecord : esParser.getRecords()) {
				if (!csvRecord.get("Event Set Name").isEmpty()) {
					parentNameReference.set(csvRecord.get("Event Set Name"));
					if (!csvRecord.get("Child Set Name").isEmpty()) {
						esNavigableData.add(new NavigableExtract(
								Type.EVENT_SET,
								csvRecord.get("Child Set Name"),
								parentNameReference.get()));
					}
				} else if (!csvRecord.get("Child Set Name").isEmpty()) {
					esNavigableData.add(new NavigableExtract(
							Type.EVENT_SET,
							csvRecord.get("Child Set Name"),
							parentNameReference.get()));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}
		return esNavigableData;
	}

	private List<NavigableExtract> ecNavigableData(File ecCSV) {
		List<NavigableExtract> ecNavigableData = new ArrayList<>();
		try (Reader ecReader = new FileReader(ecCSV);
			 CSVParser ecParser = csvFormat.parse(ecReader)) {
			for (CSVRecord csvRecord : ecParser.getRecords()) {
				if (!csvRecord.get("Prev Display").isEmpty()) {
					ecNavigableData.add(new NavigableExtract(
							Type.EVENT_CODE,
							csvRecord.get("Code Value"),
							csvRecord.get("Event Set Name")));
				}
			}
		} catch (IOException ioException) {
			throw new RuntimeException(ioException);
		}
		return ecNavigableData;
	}
}
