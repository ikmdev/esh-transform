package dev.ikm.ike.tinkarizer.extract;

import dev.ikm.ike.tinkarizer.entity.NavigableData;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import org.apache.commons.csv.CSVRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static dev.ikm.ike.tinkarizer.entity.Namespace.ES_NAMESPACE;

public class ESNavigableIterable implements Iterable<List<NavigableData>> {

	private final Iterator<CSVRecord> csvIterator;
	private final int batchSize;

	AtomicReference<String> parentNameReference = new AtomicReference<>();
	AtomicReference<UUID> parentReference = new AtomicReference<>();

	public ESNavigableIterable(Iterator<CSVRecord> csvIterator, int batchSize) {
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
					if (!csvRecord.get("Event Set Name").isEmpty()) {
						UUID parentId = UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Event Set Name"));
						parentNameReference.set(csvRecord.get("Event Set Name"));
						parentReference.set(parentId);
						if (!csvRecord.get("Child Set Name").isEmpty()) {
							UUID childId = UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Child Set Name"));
							batch.add(new NavigableData(csvRecord.get("Child Set Name"), childId, parentNameReference.get(), parentId));
						}
					} else if (!csvRecord.get("Child Set Name").isEmpty()) {
						UUID childId = UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Child Set Name"));
						batch.add(new NavigableData(csvRecord.get("Child Set Name"), childId, parentNameReference.get(), parentReference.get()));
					}
				}
				return batch;
			}
		};
	}
}
