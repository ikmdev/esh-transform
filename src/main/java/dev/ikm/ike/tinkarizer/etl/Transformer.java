package dev.ikm.ike.tinkarizer.etl;

import dev.ikm.ike.tinkarizer.entity.Namespace;
import dev.ikm.ike.tinkarizer.entity.NavigableDatum;
import dev.ikm.ike.tinkarizer.entity.NavigableExtract;
import dev.ikm.ike.tinkarizer.entity.Type;
import dev.ikm.ike.tinkarizer.entity.ViewableDatum;
import dev.ikm.ike.tinkarizer.entity.ViewableExtract;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Transformer {

	private final List<UUID> extractedESIds;
	private final List<NavigableExtract> skippedNavigableExtracts;

	public Transformer() {
		this.extractedESIds = new ArrayList<>();
		this.skippedNavigableExtracts = new ArrayList<>();
	}

	public List<NavigableExtract> getSkippedNavigableExtracts() {
		return skippedNavigableExtracts;
	}

	public List<ViewableDatum> transformViewableExtracts(List<ViewableExtract> viewableExtracts) {
		return viewableExtracts.stream()
				.map(extract -> switch (extract.type()) {
					case EVENT_SET -> {
						List<UUID> conceptIds = computeId(Type.EVENT_SET, List.of(extract.fqn()));
						extractedESIds.addAll(conceptIds);
						yield new ViewableDatum(conceptIds, true, extract.fqn(), extract.syn(), extract.def(), "");
					}
					case EVENT_CODE ->  {
						List<UUID> conceptIds = computeId(Type.EVENT_CODE, List.of(extract.id()));
						boolean isActive = extract.status().equalsIgnoreCase("active");
						extractedESIds.addAll(conceptIds);
						yield new ViewableDatum(conceptIds, isActive, extract.fqn(), extract.syn(), extract.def(), extract.id());
					}
				})
				.toList();
	}

	public List<NavigableDatum> transformNavigableExtracts(List<NavigableExtract> navigableExtracts) {
		//First pass to clean up all data and capture multi-parent is-a relationships for ES Data
		List<NavigableDatum> navigableData = new ArrayList<>();
		Map<UUID, List<UUID>> esIsA = new HashMap<>();
		for (NavigableExtract extract : navigableExtracts) {
			switch (extract.type()) {
				case EVENT_SET -> {
					UUID childId = computeId(Type.EVENT_SET, extract.childId());
					UUID parentId = computeId(Type.EVENT_SET, extract.parentId());
					if (!esIsA.containsKey(childId)) {
						List<UUID> parents = new ArrayList<>();
						parents.add(parentId);
						esIsA.put(childId, parents);
					} else {
						esIsA.get(childId).add(parentId);
					}
				}
				case EVENT_CODE -> {
					UUID childId = computeId(Type.EVENT_CODE, extract.childId());
					UUID parentId = computeId(Type.EVENT_SET, extract.parentId());
					if (extractedESIds.contains(parentId)) {
						navigableData.add(new NavigableDatum(childId, List.of(parentId)));
					} else {
						skippedNavigableExtracts.add(extract);
					}
				}
			}
		}

		//Transform consolidated ES Navigation data
		esIsA.forEach((childId, parentIds) -> navigableData.add(new NavigableDatum(childId, parentIds)));
		return navigableData;
	}

	private List<UUID> computeId(Type type, List<String> ids) {
		return ids.stream()
				.map(identifier -> computeId(type, identifier))
				.toList();
	}

	private UUID computeId(Type type, String id) {
		return switch (type) {
			case EVENT_SET ->  UuidT5Generator.get(Namespace.ES_NAMESPACE, id);
			case EVENT_CODE -> UuidT5Generator.get(Namespace.EC_NAMESPACE, id);
		};
	}
}
