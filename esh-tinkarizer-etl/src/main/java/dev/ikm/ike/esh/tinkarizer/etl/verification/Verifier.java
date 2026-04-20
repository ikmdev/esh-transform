package dev.ikm.ike.esh.tinkarizer.etl.verification;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;

public interface Verifier {

	void verifyViewableData(List<ViewableCanonicalRecord> viewableCanonicalRecords);

	void verifyNavigableData(List<NavigableCanonicalRecord> navigableCanonicalRecords);

}
