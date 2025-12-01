package dev.ikm.ike.tinkarizer.extract;

import dev.ikm.ike.tinkarizer.entity.ViewableData;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import org.apache.commons.csv.CSVRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static dev.ikm.ike.tinkarizer.entity.Namespace.ES_NAMESPACE;

public class ESViewableIterable implements Iterable<List<ViewableData>> {

	private final Iterator<CSVRecord> csvIterator;
	private final int batchSize;

	public ESViewableIterable(Iterator<CSVRecord> csvIterator, int batchSize) {
		this.csvIterator = csvIterator;
		this.batchSize = batchSize;
	}

	@Override
	public Iterator<List<ViewableData>> iterator() {
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return csvIterator.hasNext();
			}

			@Override
			public List<ViewableData> next() {
				List<ViewableData> batch = new ArrayList<>(batchSize);
				while (csvIterator.hasNext() && batch.size() < batchSize) {
					CSVRecord csvRecord = csvIterator.next();
					if (!csvRecord.get("Event Set Name").isEmpty()) {
						batch.add(new ViewableData(
								UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Event Set Name")),
								true,
								csvRecord.get("Event Set Name"),
								csvRecord.get("Event Set Disp"),
								csvRecord.get("Event Set Descr"),
								""));
					}
				}
				return batch;
			}
		};
	}


}
