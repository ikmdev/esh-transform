package dev.ikm.ike.esh.tinkarizer.etl.transform;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;

public interface Transformer {

	List<ViewableCanonicalRecord> transformViewables(List<ViewableSourceRecord> viewableSourceRecord);

	List<NavigableCanonicalRecord> transformNavigables(List<NavigableSourceRecord> navigableSourceRecord);

}
