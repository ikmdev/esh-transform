package dev.ikm.ike.tinkarizer.extract;

import dev.ikm.ike.tinkarizer.entity.ViewableData;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import org.apache.commons.csv.CSVRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static dev.ikm.ike.tinkarizer.entity.Namespace.EC_NAMESPACE;

public class ECViewableIterable implements Iterable<List<ViewableData>>{

	private final Iterator<CSVRecord> csvIterator;
	private final int batchSize;

	public ECViewableIterable(Iterator<CSVRecord> csvIterator, int batchSize) {
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
								List.of(UuidT5Generator.get(EC_NAMESPACE,
										csvRecord.get("Code Value") + "|" + csvRecord.get("Prev Display"))),
								csvRecord.get("Status").equalsIgnoreCase("active"),
								csvRecord.get("Prev Display"),
								csvRecord.get("Description"),
								csvRecord.get("Definition"),
								csvRecord.get("Code Value")));
					}
				}
				return batch;
			}
		};
	}
}
