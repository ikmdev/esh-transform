package dev.ikm.ike.esh.tinkarizer.event.code;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import dev.ikm.ike.esh.tinkarizer.etl.domain.Id;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.index.EntityIndex;
import dev.ikm.ike.esh.tinkarizer.etl.transformer.Transformer;
import dev.ikm.ike.esh.tinkarizer.starter.data.ESHStarterData;

public class EventCodeTransformer implements Transformer {

	private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(EventCodeTransformer.class);

	private final EntityIndex entityIndex;

	public EventCodeTransformer(EntityIndex entityIndex) {
		this.entityIndex = entityIndex;
	}

	@Override
	public List<ViewableCanonicalRecord> transformViewables(List<ViewableSourceRecord> viewableExtracts) {
		List<ViewableCanonicalRecord> viewableCanonicalRecords = new ArrayList<>();
		for (ViewableSourceRecord viewableExtract : viewableExtracts) {
			UUID conceptId = Id.generateId(ESHStarterData.EVENT_CODE_NAMESPACE, viewableExtract.identifier());
			boolean isActive = viewableExtract.status().equalsIgnoreCase("active");
			ViewableCanonicalRecord viewableCanonicalRecord = new ViewableCanonicalRecord(
					ESHStarterData.EVENT_CODE_NAMESPACE, conceptId, isActive, viewableExtract.fqn(),
					viewableExtract.syn(), viewableExtract.def(), viewableExtract.identifier(),
					ESHStarterData.EVENT_CODE_IDENTIFIER_UUID);
			viewableCanonicalRecords.add(viewableCanonicalRecord);
		}
		return viewableCanonicalRecords;
	}

	@Override
	public List<NavigableCanonicalRecord> transformNavigables(List<NavigableSourceRecord> navigableExtracts) {
		List<NavigableCanonicalRecord> navigableCanonicalRecords = new ArrayList<>();
		Map<UUID, List<UUID>> esIsA = new HashMap<>();

		for (NavigableSourceRecord navigableExtract : navigableExtracts) {
			if (!entityIndex.exists(ESHStarterData.EVENT_SET_NAMESPACE, navigableExtract.parentId())) {
				LOG.warn("Missing parent concept for child: {}", navigableExtract.childId());
				continue;
			}
			UUID childId = Id.generateId(ESHStarterData.EVENT_CODE_NAMESPACE, navigableExtract.childId());
			UUID parentId = Id.generateId(ESHStarterData.EVENT_CODE_NAMESPACE, navigableExtract.parentId());
			if (!esIsA.containsKey(childId)) {
				List<UUID> parents = new ArrayList<>();
				parents.add(parentId);
				esIsA.put(childId, parents);
			} else {
				esIsA.get(childId).add(parentId);
			}
		}

		//Build NavigableCanonicalRecords
		esIsA.forEach((childId, parentIds) -> {
			NavigableCanonicalRecord navigableCanonicalRecord = new NavigableCanonicalRecord(
					ESHStarterData.EVENT_CODE_NAMESPACE, childId, parentIds);
			navigableCanonicalRecords.add(navigableCanonicalRecord);
		});

		return navigableCanonicalRecords;
	}

}
