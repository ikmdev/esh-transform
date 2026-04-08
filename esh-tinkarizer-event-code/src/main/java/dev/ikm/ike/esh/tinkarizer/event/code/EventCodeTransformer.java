package dev.ikm.ike.esh.tinkarizer.event.code;

import java.util.List;
import java.util.UUID;

import dev.ikm.ike.esh.tinkarizer.etl.Transformer;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;

public class EventCodeTransformer implements Transformer {

	private final List<UUID> extractedESIds;

	public EventCodeTransformer(List<UUID> extractedESIds) {
		this.extractedESIds = extractedESIds;
	}

	@Override
	public List<NavigableCanonicalRecord> transformNavigables(List<NavigableSourceRecord> extracts) {
		
	}

	@Override
	public List<ViewableCanonicalRecord> transformViewables(List<ViewableSourceRecord> extracts) {
		return extracts.stream()
				.map(extract -> switch (extract.type()) {
					case EVENT_SET -> {
						List<UUID> conceptIds = computeId(Type.EVENT_SET, List.of(extract.fqn()));
						extractedESIds.addAll(conceptIds);
						yield new ViewableSourceRecord(conceptIds, true, extract.fqn(), extract.syn(), extract.def(), "");
					}
					case EVENT_CODE ->  {
						List<UUID> conceptIds = computeId(Type.EVENT_CODE, List.of(extract.id()));
						boolean isActive = extract.status().equalsIgnoreCase("active");
						extractedESIds.addAll(conceptIds);
						yield new ViewableSourceRecord(conceptIds, isActive, extract.fqn(), extract.syn(), extract.def(), extract.id());
					}
				})
				.toList();
	}

}
