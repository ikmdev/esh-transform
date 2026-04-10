package dev.ikm.ike.esh.tinkarizer.etl.transformer;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;

public interface Transformer {

	List<ViewableCanonicalRecord> transformViewables(List<ViewableSourceRecord> extracts);

	List<NavigableCanonicalRecord> transformNavigables(List<NavigableSourceRecord> extracts);

}
