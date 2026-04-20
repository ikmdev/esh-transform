package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Gatherers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ikm.ike.esh.tinkarizer.etl.load.LoadConfig;
import dev.ikm.ike.esh.tinkarizer.etl.load.Writer;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.transaction.Transaction;
import dev.ikm.tinkar.terms.State;

public class BatchPipeline extends BasePipeline {

	private final static Logger LOG = LoggerFactory.getLogger(BatchPipeline.class);

	private final int batchSize;
	private final ExecutorService executorService = Executors.newFixedThreadPool(4);

	public BatchPipeline(PipelineConfig pipelineConfig, LoadConfig writeConfig, int batchSize) {
		super(pipelineConfig, writeConfig);
		this.batchSize = batchSize;
	}

	@Override
	public String getName() {
		return pipelineConfig.pipelineName();
	}

	@Override
	protected void loadNavigableData() {
		Transaction transaction = new Transaction("Load Navigable Data");

		StampEntity<?> activeStampEntity = transaction.getStamp(State.ACTIVE, loadConfig.time(),
				loadConfig.authorPublicId(), loadConfig.modulePublicId(), loadConfig.pathPublicId());
		StampEntity<?> inactiveStampEntity = transaction.getStamp(State.INACTIVE, loadConfig.time(),
				loadConfig.authorPublicId(), loadConfig.modulePublicId(), loadConfig.pathPublicId());

		Writer writer = new Writer(transaction, activeStampEntity, inactiveStampEntity);

		loadBatches(navigableCanonicalRecords, batch -> loader.loadNavigableData(writer, batch));
		transaction.commit();
	}

	@Override
	protected void loadViewableData() {
		Transaction transaction = new Transaction("Load Viewable Data");

		StampEntity<?> activeStampEntity = transaction.getStamp(State.ACTIVE, loadConfig.time(),
				loadConfig.authorPublicId(), loadConfig.modulePublicId(), loadConfig.pathPublicId());
		StampEntity<?> inactiveStampEntity = transaction.getStamp(State.INACTIVE, loadConfig.time(),
				loadConfig.authorPublicId(), loadConfig.modulePublicId(), loadConfig.pathPublicId());
				
		Writer writer = new Writer(transaction, activeStampEntity, inactiveStampEntity);

		loadBatches(viewableCanonicalRecords, batch -> loader.loadViewableData(writer, batch));
		transaction.commit();
	}

	private <T> void loadBatches(List<T> records, Consumer<List<T>> loaderFn) {
		List<CompletableFuture<Void>> futures = records.stream().gather(Gatherers.windowFixed(batchSize)).map(
				batch -> CompletableFuture.runAsync(() -> loaderFn.accept(batch), executorService).exceptionally(ex -> {
					LOG.error("Error loading batch", ex);
					return null;
				})).toList();

		CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
	}
}
