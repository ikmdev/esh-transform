package dev.ikm.ike.tinkarizer.extract;

import dev.ikm.ike.tinkarizer.Debug;
import dev.ikm.ike.tinkarizer.entity.NavigableData;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static dev.ikm.ike.tinkarizer.entity.Namespace.EC_NAMESPACE;
import static dev.ikm.ike.tinkarizer.entity.Namespace.ES_NAMESPACE;

public class ECNavigableIterable implements Iterable<List<NavigableData>> {

	private final Logger LOG = LoggerFactory.getLogger(ECNavigableIterable.class);

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
					if (!csvRecord.get("Prev Display").isEmpty()) {
//						String parentEventSetName = determineEventSetParentName(csvRecord);
						//Debugging
						if (Debug.esViewableIds.contains(UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Event Set Name")))) {
							batch.add(new NavigableData(
									csvRecord.get("Prev Display"),
									UuidT5Generator.get(EC_NAMESPACE, csvRecord.get("Code Value") + "|" + csvRecord.get("Prev Display")),
									csvRecord.get("Event Set Name"),
									UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Event Set Name"))));
						} else {
							Debug.ecParents.put(UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Event Set Name")), csvRecord.get("Event Set Name"));
						}
						//
//						batch.add(new NavigableData(
//								csvRecord.get("Prev Display"),
//								UuidT5Generator.get(EC_NAMESPACE, csvRecord.get("Code Value") + "|" + csvRecord.get("Prev Display")),
//								parentEventSetName,
//								UuidT5Generator.get(ES_NAMESPACE, parentEventSetName)));
//						batch.add(new NavigableData(
//								csvRecord.get("Prev Display"),
//								UuidT5Generator.get(EC_NAMESPACE, csvRecord.get("Code Value") + "|" + csvRecord.get("Prev Display")),
//								csvRecord.get("Event Set Name"),
//								UuidT5Generator.get(ES_NAMESPACE, csvRecord.get("Event Set Name"))));
					}
				}
				return batch;
			}
		};
	}

	private String determineEventSetParentName(CSVRecord csvRecord) {
		if (isAllUpperCase(csvRecord.get("Event Set Name"))) {
			return  csvRecord.get("Prev Display");
		} else {
			return csvRecord.get("Event Set Name");
		}
	}

	public boolean isAllUpperCase(String s) {
		for (char c : s.toCharArray()) { // Iterate through all characters
			if (Character.isLetter(c) && !Character.isUpperCase(c)) {
				// If the character is a letter but not uppercase, it's not all caps
				return false;
			}
		}
		return true; // All relevant characters were uppercase (or not letters)
	}
}
