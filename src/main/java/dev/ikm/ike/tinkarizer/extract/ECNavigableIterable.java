package dev.ikm.ike.tinkarizer.extract;

import dev.ikm.ike.tinkarizer.entity.NavigableData;
import dev.ikm.ike.tinkarizer.entity.ViewableData;
import org.apache.commons.csv.CSVRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ECNavigableIterable implements Iterable<List<NavigableData>> {

	private final Iterator<CSVRecord> csvIterator;
	private final int batchSize;

	public ECNavigableIterable(Iterator<CSVRecord> csvIterator, int batchSize) {
		this.csvIterator = csvIterator;
		this.batchSize = batchSize;
	}

	@Override
	public Iterator<List<NavigableData>> iterator() {
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return csvIterator.hasNext();
			}

			@Override
			public List<NavigableData> next() {
				List<NavigableData> batch = new ArrayList<>(batchSize);
				while (csvIterator.hasNext() && batch.size() < batchSize) {
					CSVRecord csvRecord = csvIterator.next();

				}
				return batch;
			}
		};
	}
}
