package dev.ikm.ike.esh.tinkarizer.event.set;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.transformer.Transformer;

public class EventSetTransformer implements Transformer {

	@Override
	public List<NavigableCanonicalRecord> transformNavigables(List<NavigableSourceRecord> extracts) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ViewableCanonicalRecord> transformViewables(List<ViewableSourceRecord> extracts) {
		// TODO Auto-generated method stub
		return null;
	}

}
