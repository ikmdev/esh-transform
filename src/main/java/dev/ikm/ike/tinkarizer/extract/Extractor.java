package dev.ikm.ike.tinkarizer.extract;

import dev.ikm.ike.tinkarizer.entity.NavigableData;
import dev.ikm.ike.tinkarizer.entity.ViewableData;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class Extractor implements AutoCloseable{

	private final CSVFormat csvFormat;
	private ArrayList<CSVParser> parsers;
	private final File esCSV;
	private final File ecCSV;
	private final int batchSize;

	public Extractor(File esCSV, File ecCSV, int batchSize) {
		Objects.requireNonNull(esCSV);
		Objects.requireNonNull(ecCSV);
		this.esCSV = esCSV;
		this.ecCSV = ecCSV;
		this.batchSize = batchSize;
		this.csvFormat = CSVFormat.DEFAULT.builder()
				.setHeader()
				.setSkipHeaderRecord(true)
				.get();
		this.parsers = new ArrayList<>();
	}

	public Iterable<List<ViewableData>> viewableData() throws IOException {
		ESViewableIterable esViewableIterable = new ESViewableIterable(processES(), batchSize);
		ECViewableIterable ecViewableIterable = new ECViewableIterable(processEC(), batchSize);
		return IterableUtils.chainedIterable(esViewableIterable, ecViewableIterable);
	}

	public Iterable<List<NavigableData>> navigableData() throws IOException {
		ESNavigableIterable esViewableIterable = new ESNavigableIterable(processES(), batchSize);
		ECNavigableIterable ecViewableIterable = new ECNavigableIterable(processEC(), batchSize);
		return IterableUtils.chainedIterable(esViewableIterable, ecViewableIterable);
	}

	private Iterator<CSVRecord> processES() throws IOException {
		Reader esReader = new FileReader(esCSV);
		CSVParser esParser = csvFormat.parse(esReader);
		parsers.add(esParser);
		return  esParser.iterator();
	}

	private Iterator<CSVRecord> processEC() throws IOException {
		Reader ecReader = new FileReader(ecCSV);
		CSVParser ecParser = csvFormat.parse(ecReader);
		parsers.add(ecParser);
		return ecParser.iterator();
	}

	@Override
	public void close() throws Exception {
		for (CSVParser csvParser : parsers) {
			csvParser.close();
		}
	}
}
