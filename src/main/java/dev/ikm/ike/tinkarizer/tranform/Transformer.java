package dev.ikm.ike.tinkarizer.tranform;

import dev.ikm.ike.tinkarizer.entity.NavigableData;
import dev.ikm.ike.tinkarizer.entity.ViewableData;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.template.Definition;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.composer.template.Synonym;
import dev.ikm.tinkar.composer.template.USDialect;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.EntityProxy.Semantic;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTermV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class Transformer implements AutoCloseable{

	Logger LOG = LoggerFactory.getLogger(Transformer.class);

	private static final Concept ESH_AUTHOR_CONCEPT = Concept.make(PublicIds.of(UUID.fromString("5ffd229a-aaa1-4a93-9577-8950f86a2454")));
	private static final Concept ESH_MODULE_CONCEPT = Concept.make(PublicIds.of(UUID.fromString("fd7b189b-eeba-4a41-98bb-b101cf80ef02")));
	private static final Concept ESH_MODEL_CONCEPT = Concept.make(PublicIds.of(UUID.fromString("f0b69a19-ba4f-4e52-b30e-d998f028f0ab")));
	private static final Concept EC_IDENTIFIER_CONCEPT = Concept.make(PublicIds.of(UUID.fromString("9900bd6e-42d4-4484-80f2-cea339c956b5")));

	private final Composer composer;
	private final Session activeSession;
	private final Session inactiveSession;

	private final AtomicInteger conceptCounter;
	private final AtomicInteger semanticCounter;


	public Transformer(long stampTime) {
		this.composer = new Composer("Event Set Hierarchy");
		this.activeSession = composer.open(
				State.ACTIVE,
				stampTime,
				ESH_AUTHOR_CONCEPT,
				ESH_MODULE_CONCEPT,
				TinkarTermV2.DEVELOPMENT_PATH);
		this.inactiveSession = composer.open(
				State.INACTIVE,
				stampTime,
				ESH_AUTHOR_CONCEPT,
				ESH_MODULE_CONCEPT,
				TinkarTermV2.DEVELOPMENT_PATH);
		this.conceptCounter = new AtomicInteger(0);
		this.semanticCounter = new AtomicInteger(0);
		init();
	}

	private void init() {
		activeSession.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.concept(EC_IDENTIFIER_CONCEPT)
				.attach((FullyQualifiedName fqn) -> fqn
						.language(TinkarTermV2.ENGLISH_LANGUAGE)
						.text("Event Code Identifier")
						.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE)
						.attach((USDialect dialect) -> dialect.acceptability(TinkarTermV2.PREFERRED)))
				.attach((Synonym syn) -> syn
						.language(TinkarTermV2.ENGLISH_LANGUAGE)
						.text("EC Identifier")
						.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE)
						.attach((USDialect dialect) -> dialect.acceptability(TinkarTermV2.PREFERRED)))
				.attach((Definition def) -> def
						.language(TinkarTermV2.ENGLISH_LANGUAGE)
						.text("Identifier to enable uniqueness between Event Codes.")
						.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE)
						.attach((USDialect dialect) -> dialect.acceptability(TinkarTermV2.PREFERRED)))
				.attach((StatedAxiom stated) -> stated.isA(TinkarTermV2.IDENTIFIER_SOURCE)));
	}

	public void viewableTransformation(List<ViewableData> viewableData) {

		for (ViewableData data : viewableData) {
			Concept concept = Concept.make(PublicIds.of(data.ids()));

			//Create Concept Active or Inactive
			if (data.isActive()) {
				activeSession.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.publicId(concept.publicId()));
				conceptCounter.incrementAndGet();
			} else {
				inactiveSession.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.publicId(concept.publicId()));
				conceptCounter.incrementAndGet();
			}

			//Create FQN Semantic
			Semantic fqn = Semantic.make(PublicIds.newRandom());
			activeSession.compose(new FullyQualifiedName()
					.semantic(fqn)
					.language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.fqn())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			semanticCounter.incrementAndGet();
			activeSession.compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), fqn);
			semanticCounter.incrementAndGet();

			//Create SYN Semantic
			Semantic syn = Semantic.make(PublicIds.newRandom());
			activeSession.compose(new Synonym()
					.semantic(syn)
					.language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.syn().isEmpty() ? data.fqn() : data.syn())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			semanticCounter.incrementAndGet();
			activeSession.compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), syn);
			semanticCounter.incrementAndGet();

			//Create DEF Semantic
			Semantic def = Semantic.make(PublicIds.newRandom());
			activeSession.compose(new Definition()
					.semantic(def)
					.language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.def().isEmpty() ? data.fqn() : data.def())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			semanticCounter.incrementAndGet();
			activeSession.compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), def);
			semanticCounter.incrementAndGet();

			//Create Identifier Semantic
			if (!data.identifier().isEmpty()) {
				activeSession.compose(new Identifier().source(EC_IDENTIFIER_CONCEPT).identifier(data.identifier()), concept);
				semanticCounter.incrementAndGet();
			}
		}
		LOG.info("Finish transforming {} Viewable Data", viewableData.size());
	}

	public void navigableTransformation(List<NavigableData> navigableData) {
		//Need Stated Relationship to tie to main hierarchy
		activeSession.compose(new StatedAxiom().isA(ESH_MODEL_CONCEPT), Concept.make(PublicIds.of(UUID.fromString("47e533f4-a3d8-5d5b-826d-24eb09d1f3ab"))));

		for (NavigableData navData : navigableData) {
			Concept reference = Concept.make(PublicIds.of(navData.childId()));
			if (navData.parentId() == null) {
				System.out.println("break");
			}
			Concept parent = Concept.make(PublicIds.of(navData.parentId()));
			activeSession.compose(new StatedAxiom().isA(parent), reference);
		}
		LOG.info("Finish transforming {} Navigable Data", navigableData.size());
	}

	@Override
	public void close() throws Exception {
		composer.commitAllSessions();
	}
}
