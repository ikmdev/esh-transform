package dev.ikm.ike.esh.tinkarizer.etl.load;

import java.util.UUID;

import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.entity.ConceptEntity;
import dev.ikm.tinkar.entity.ConceptEntityVersion;
import dev.ikm.tinkar.entity.ConceptRecord;
import dev.ikm.tinkar.entity.ConceptRecordBuilder;
import dev.ikm.tinkar.entity.ConceptVersionRecord;
import dev.ikm.tinkar.entity.ConceptVersionRecordBuilder;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.RecordListBuilder;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.SemanticRecord;
import dev.ikm.tinkar.entity.SemanticRecordBuilder;
import dev.ikm.tinkar.entity.SemanticVersionRecord;
import dev.ikm.tinkar.entity.SemanticVersionRecordBuilder;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.transaction.Transaction;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.EntityProxy.Pattern;
import dev.ikm.tinkar.terms.EntityProxy.Semantic;

public class Writer {

	private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

	private final Transaction transaction;
	private final StampEntity<?> activeStamp;
	private final StampEntity<?> inactiveStamp;

	public Writer(Transaction transaction, StampEntity<?> activeStampEntity, StampEntity<?> inactiveStampEntity) {
		this.transaction = transaction;
		this.activeStamp = activeStampEntity;
		this.inactiveStamp = inactiveStampEntity;
	}

	private void writeEntity(Entity entity) {
		EntityService.get().putEntity(entity);
	}

	private long[] createAdditionalLongs(PublicId publicId) {
		long[] additionalLongs = new long[(publicId.uuidCount() * 2) - 2];
		int index = 0;
		for (int i = 1; i < publicId.uuidCount(); i++) {
			UUID uuid = publicId.asUuidArray()[i];
			additionalLongs[index++] = uuid.getMostSignificantBits();
			additionalLongs[index++] = uuid.getLeastSignificantBits();
		}
		return additionalLongs.length == 0 ? null : additionalLongs;
	}

	public void writeConcept(Concept concept, boolean isActive) {
		// Pull out primordial UUID from PublicId
		UUID primordialUUID = concept.asUuidArray()[0];

		// Process additional UUID longs from PublicId
		ImmutableLongList additionalLongs = LongLists.immutable.of(createAdditionalLongs(concept));

		// Create empty version list
		RecordListBuilder<ConceptVersionRecord> versions = RecordListBuilder.make();

		// Assign nids for PublicIds
		int stampNid = EntityService.get().nidForPublicId(isActive ? activeStamp : inactiveStamp);

		// Create Concept Chronology
		ConceptRecord conceptRecord = ConceptRecordBuilder.builder().nid(concept.nid())
				.leastSignificantBits(primordialUUID.getLeastSignificantBits())
				.mostSignificantBits(primordialUUID.getMostSignificantBits())
				.additionalUuidLongs(additionalLongs.toArray()).versions(versions).build();

		// Append Concept Version
		versions.add(ConceptVersionRecordBuilder.builder().chronology(conceptRecord).stampNid(stampNid).build());

		// Rebuild the ConceptRecord with the now populated version data
		ConceptEntity<? extends ConceptEntityVersion> conceptEntity = ConceptRecordBuilder.builder(conceptRecord)
				.versions(versions.toImmutable()).build();
		writeEntity(conceptEntity);
		transaction.addComponent(concept.nid());
	}

	public void writeSemantic(Semantic semantic, boolean isActive, EntityProxy referencedComponent, Pattern pattern,
			ImmutableList<Object> fieldValues) {
		// Assign primordial UUID from PublicId
		UUID primordialUUID = semantic.asUuidArray()[0];

		// Process additional UUID longs from PublicId
		ImmutableLongList additionalLongs = LongLists.immutable.of(createAdditionalLongs(semantic));

		// Create empty version list
		RecordListBuilder<SemanticVersionRecord> versions = RecordListBuilder.make();

		// Assign nids for PublicIds
		int stampNid = EntityService.get().nidForPublicId(isActive ? activeStamp : inactiveStamp);

		// Create Semantic Chronology
		SemanticRecord semanticRecord = SemanticRecordBuilder.builder().nid(semantic.nid())
				.leastSignificantBits(primordialUUID.getLeastSignificantBits())
				.mostSignificantBits(primordialUUID.getMostSignificantBits())
				.additionalUuidLongs(additionalLongs.toArray()).patternNid(pattern.nid())
				.referencedComponentNid(referencedComponent.nid()).versions(versions.toImmutable()).build();

		// Append new Semantic Version
		versions.add(SemanticVersionRecordBuilder.builder().chronology(semanticRecord).stampNid(stampNid)
				.fieldValues(fieldValues).build());

		// Rebuild the Semantic with the now populated version data
		SemanticEntity<? extends SemanticEntityVersion> semanticEntity = SemanticRecordBuilder.builder(semanticRecord)
				.versions(versions.toImmutable()).build();
		writeEntity(semanticEntity);
		transaction.addComponent(semantic.nid());
	}
}