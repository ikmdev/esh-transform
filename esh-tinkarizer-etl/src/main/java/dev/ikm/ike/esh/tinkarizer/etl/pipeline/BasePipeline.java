package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.extract.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.initialize.Initializer;
import dev.ikm.ike.esh.tinkarizer.etl.load.LoadConfig;
import dev.ikm.ike.esh.tinkarizer.etl.load.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.transform.Transformer;
import dev.ikm.ike.esh.tinkarizer.etl.validation.Validator;
import dev.ikm.ike.esh.tinkarizer.etl.verification.Verification;

public abstract class BasePipeline implements Pipeline {

	Logger LOG = org.slf4j.LoggerFactory.getLogger(this.getClass());

	protected final Initializer initializer;
	protected final Extractor extractor;
	protected final Transformer transformer;
	protected final Loader loader;
	protected final Validator validator;
	protected final Verification verification;
	protected final PipelineConfig pipelineConfig;
	protected final LoadConfig writeConfig;

	protected final List<ViewableSourceRecord> viewableSourceRecords;
	protected final List<NavigableSourceRecord> navigableSourceRecords;
	protected final List<ViewableCanonicalRecord> viewableCanonicalRecords;
	protected final List<NavigableCanonicalRecord> navigableCanonicalRecords;

	public BasePipeline(PipelineConfig pipelineConfig, LoadConfig writeConfig) {
		this.pipelineConfig = Objects.requireNonNull(pipelineConfig, "pipelineConfig cannot be null");
		this.writeConfig = Objects.requireNonNull(writeConfig, "writeConfig cannot be null");
		this.extractor = Objects.requireNonNull(pipelineConfig.extractor(), "extractor cannot be null");
		this.transformer = Objects.requireNonNull(pipelineConfig.transformer(), "transformer cannot be null");
		this.loader = Objects.requireNonNull(pipelineConfig.loader(), "loader cannot be null");
		this.validator = Objects.requireNonNull(pipelineConfig.validator(), "validator cannot be null");
		this.verification = Objects.requireNonNull(pipelineConfig.verification(), "verification cannot be null");
		this.initializer = Objects.requireNonNull(pipelineConfig.initializer(), "initializer cannot be null");
		this.viewableSourceRecords = new ArrayList<>();
		this.navigableSourceRecords = new ArrayList<>();
		this.viewableCanonicalRecords = new ArrayList<>();
		this.navigableCanonicalRecords = new ArrayList<>();
	}

	@Override
	public void run() {
		Instant pipelineStartTime = Instant.now();

		Instant initializationStart = Instant.now();
		performInitialization();
		Instant initializationEnd = Instant.now();
		Duration initializationDuration = Duration.between(initializationStart, initializationEnd);
		LOG.info("Initialization complete for pipeline: {} in {} minutes {} seconds", getName(),
				initializationDuration.toMinutes(), initializationDuration.getSeconds());

		Instant extractionStart = Instant.now();
		performExtraction();
		Instant extractionEnd = Instant.now();
		Duration extractionDuration = Duration.between(extractionStart, extractionEnd);
		LOG.info("Extraction complete for pipeline: {} viewable {} navigable {} in {} minutes {} seconds", getName(),
				viewableSourceRecords.size(), navigableSourceRecords.size(), extractionDuration.toMinutes(),
				extractionDuration.getSeconds());

		Instant transformationStart = Instant.now();
		performTransformation();
		Instant transformationEnd = Instant.now();
		Duration transformationDuration = Duration.between(transformationStart, transformationEnd);
		LOG.info("Transformation complete for pipeline:{} viewable {} navigable {} in {} minutes {} seconds", getName(),
				viewableCanonicalRecords.size(), navigableCanonicalRecords.size(), transformationDuration.toMinutes(),
				transformationDuration.getSeconds());

		Instant validationStart = Instant.now();
		performValidation();
		Instant validationEnd = Instant.now();
		Duration validationDuration = Duration.between(validationStart, validationEnd);
		LOG.info("Validation complete for pipeline: {} in {} minutes {} seconds", getName(),
				validationDuration.toMinutes(), validationDuration.getSeconds());

		Instant loadStart = Instant.now();
		performLoad();
		Instant loadEnd = Instant.now();
		Duration loadDuration = Duration.between(loadStart, loadEnd);
		LOG.info("Load complete for pipeline: {} in {} minutes {} seconds", getName(), loadDuration.toMinutes(),
				loadDuration.getSeconds());

		Instant verificationStart = Instant.now();
		performVerification();
		Instant verificationEnd = Instant.now();
		Duration verificationDuration = Duration.between(verificationStart, verificationEnd);
		LOG.info("Verification complete for pipeline: {} in {} minutes {} seconds", getName(),
				verificationDuration.toMinutes(), verificationDuration.getSeconds());

		Instant pipelineEndTime = Instant.now();
		Duration duration = Duration.between(pipelineStartTime, pipelineEndTime);
		LOG.info("Pipeline {} completed in {} minutes {} seconds", getName(), duration.toMinutes(),
				duration.getSeconds());
	}

	@Override
	public void performExtraction() {
		extractViewableData();
		extractNavigableData();
	}

	protected void extractViewableData() {
		viewableSourceRecords.addAll(extractor.extractViewableData());
	}

	protected void extractNavigableData() {
		navigableSourceRecords.addAll(extractor.extractNavigableData());
	}

	@Override
	public void performInitialization() {
		initializer.initialize();
	}

	@Override
	public void performLoad() {
		loadViewableData();
		loadNavigableData();
	}

	protected void loadViewableData() {
		loader.loadViewableData(null, viewableCanonicalRecords);
	}

	protected void loadNavigableData() {
		loader.loadNavigableData(null, navigableCanonicalRecords);
	}

	@Override
	public void performTransformation() {
		transformViewableData();
		transformNavigableData();
	}

	protected void transformViewableData() {
		viewableCanonicalRecords.addAll(transformer.transformViewables(viewableSourceRecords));
	}

	protected void transformNavigableData() {
		navigableCanonicalRecords.addAll(transformer.transformNavigables(navigableSourceRecords));
	}

	@Override
	public void performValidation() {
		validateViewableData();
		validateNavigableData();
	}

	protected void validateViewableData() {
		validator.validateViewableData(viewableCanonicalRecords);
	}

	protected void validateNavigableData() {
		validator.validateNavigableData(navigableCanonicalRecords);
	}

	@Override
	public void performVerification() {
		verifyViewableData();
		verifyNavigableData();
	}

	protected void verifyViewableData() {
		verification.verifyViewableData(viewableCanonicalRecords);
	}

	protected void verifyNavigableData() {
		verification.verifyNavigableData(navigableCanonicalRecords);
	}

}
