package dev.ikm.ike.esh.tinkarizer.event.set;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import dev.ikm.ike.esh.tinkarizer.etl.domain.Id;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.transform.Transformer;
import dev.ikm.ike.esh.tinkarizer.starter.data.ESHStarterData;

public class EventSetTransformer implements Transformer {

	@Override
	public List<ViewableCanonicalRecord> transformViewables(List<ViewableSourceRecord> viewableExtracts) {
		List<ViewableCanonicalRecord> viewableCanonicalRecords = new ArrayList<>();
		for (ViewableSourceRecord viewableExtract : viewableExtracts) {
			UUID conceptId = Id.generateId(ESHStarterData.EVENT_SET_NAMESPACE, viewableExtract.identifier());
			boolean isActive = viewableExtract.status().equalsIgnoreCase("active");
			ViewableCanonicalRecord viewableCanonicalRecord = new ViewableCanonicalRecord(
					ESHStarterData.EVENT_SET_NAMESPACE, conceptId, isActive, viewableExtract.fqn(),
					viewableExtract.syn(), viewableExtract.def(), "", null);
			viewableCanonicalRecords.add(viewableCanonicalRecord);
		}
		return viewableCanonicalRecords;
	}

	@Override
	public List<NavigableCanonicalRecord> transformNavigables(List<NavigableSourceRecord> navigableExtracts) {
		Map<UUID, NavigableCanonicalRecord> navigableRecordMap = new HashMap<>();
		
		for (NavigableSourceRecord navigableExtract : navigableExtracts) {
			UUID childId = Id.generateId(ESHStarterData.EVENT_SET_NAMESPACE, navigableExtract.childId());
			UUID parentId = Id.generateId(ESHStarterData.EVENT_SET_NAMESPACE, navigableExtract.parentId());
			if (navigableRecordMap.containsKey(childId)) {
				NavigableCanonicalRecord existingRecord = navigableRecordMap.get(childId);
				existingRecord = existingRecord.with(parentId);
				navigableRecordMap.put(childId, existingRecord);
			} else {
				NavigableCanonicalRecord navigableCanonicalRecord = new NavigableCanonicalRecord(
						ESHStarterData.EVENT_SET_NAMESPACE, navigableExtract.status().equalsIgnoreCase("active"), childId, List.of(parentId));
				navigableRecordMap.put(childId, navigableCanonicalRecord);
			}
		}

		return new ArrayList<>(navigableRecordMap.values());
	}
}
