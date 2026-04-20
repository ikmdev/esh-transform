package dev.ikm.ike.esh.tinkarizer.etl.load;

import java.util.List;

import org.eclipse.collections.api.factory.Lists;
import org.slf4j.Logger;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.EntityProxy.Semantic;
import dev.ikm.tinkar.terms.TinkarTermV2;

public class SimpleLoader implements Loader {

	private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(SimpleLoader.class);

	@Override
	public void loadNavigableData(Writer writer, List<NavigableCanonicalRecord> navigableCanonicalRecords) {
		navigableCanonicalRecords.forEach(navigableRecord -> {
			//TODO - Port over the Stated Axiom creation code
		});
	}

	@Override
	public void loadViewableData(Writer writer, List<ViewableCanonicalRecord> viewableCanonicalRecords) {
		viewableCanonicalRecords.forEach(viewableRecord -> {
			Concept concept = Concept.make(PublicIds.of(viewableRecord.conceptId()));

			// Write Concept
			writer.writeConcept(concept, viewableRecord.isActive());

			// Write Fully Qualified Name (FQN)
			if (!viewableRecord.fqn().isEmpty()) {
				Semantic fqn = Semantic.make(PublicIds.newRandom());
				writer.writeSemantic(fqn, viewableRecord.isActive(), fqn, TinkarTermV2.DESCRIPTION_PATTERN,
						Lists.immutable.of(TinkarTermV2.ENGLISH_LANGUAGE, viewableRecord.fqn(),
								TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE,
								TinkarTermV2.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE));

				// US Dialect on FQN
				Semantic fqnDialect = Semantic.make(PublicIds.newRandom());
				writer.writeSemantic(fqnDialect, viewableRecord.isActive(), fqnDialect, TinkarTermV2.US_DIALECT_PATTERN,
						Lists.immutable.of(TinkarTermV2.PREFERRED));
			}

			// Write Synonym (SYN)
			if (!viewableRecord.syn().isEmpty()) {
				Semantic syn = Semantic.make(PublicIds.newRandom());
				writer.writeSemantic(syn, viewableRecord.isActive(), syn, TinkarTermV2.DESCRIPTION_PATTERN,
						Lists.immutable.of(TinkarTermV2.ENGLISH_LANGUAGE, viewableRecord.syn(),
								TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE,
								TinkarTermV2.REGULAR_NAME_DESCRIPTION_TYPE));

				// US Dialect on SYN
				Semantic synDialect = Semantic.make(PublicIds.newRandom());
				writer.writeSemantic(synDialect, viewableRecord.isActive(), synDialect, TinkarTermV2.US_DIALECT_PATTERN,
						Lists.immutable.of(TinkarTermV2.PREFERRED));
			}

			// Write Definition (DEF)
			if (!viewableRecord.def().isEmpty()) {
				Semantic def = Semantic.make(PublicIds.newRandom());
				writer.writeSemantic(def, viewableRecord.isActive(), def, TinkarTermV2.DESCRIPTION_PATTERN,
						Lists.immutable.of(TinkarTermV2.ENGLISH_LANGUAGE, viewableRecord.def(),
								TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE, TinkarTermV2.DEFINITION_DESCRIPTION_TYPE));

				// US Dialect on DEF
				Semantic defDialect = Semantic.make(PublicIds.newRandom());
				writer.writeSemantic(defDialect, viewableRecord.isActive(), defDialect, TinkarTermV2.US_DIALECT_PATTERN,
						Lists.immutable.of(TinkarTermV2.PREFERRED));
			}

			// Write Identifier
			if (!viewableRecord.identifier().isEmpty()) {
				Semantic identifier = Semantic.make(PublicIds.newRandom());
				Concept identifierConcept = Concept.make(PublicIds.of(viewableRecord.identifierSource()));
				writer.writeSemantic(identifier, viewableRecord.isActive(), identifier, TinkarTermV2.IDENTIFIER_PATTERN,
						Lists.immutable.of(identifierConcept, viewableRecord.identifier()));
			}
		});
	}

}
