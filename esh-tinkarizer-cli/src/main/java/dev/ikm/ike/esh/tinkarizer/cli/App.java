package dev.ikm.ike.esh.tinkarizer.cli;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.slf4j.Logger;

import dev.ikm.ike.esh.tinkarizer.etl.extract.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.index.EntityIndex;
import dev.ikm.ike.esh.tinkarizer.etl.initialize.Initializer;
import dev.ikm.ike.esh.tinkarizer.etl.initialize.NoOpInitializer;
import dev.ikm.ike.esh.tinkarizer.etl.load.LoadConfig;
import dev.ikm.ike.esh.tinkarizer.etl.load.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.load.SimpleLoader;
import dev.ikm.ike.esh.tinkarizer.etl.pipeline.BatchPipeline;
import dev.ikm.ike.esh.tinkarizer.etl.pipeline.Pipeline;
import dev.ikm.ike.esh.tinkarizer.etl.pipeline.PipelineConfig;
import dev.ikm.ike.esh.tinkarizer.etl.pipeline.PipelineOrchestrator;
import dev.ikm.ike.esh.tinkarizer.etl.transform.Transformer;
import dev.ikm.ike.esh.tinkarizer.etl.validation.NoOpValidator;
import dev.ikm.ike.esh.tinkarizer.etl.validation.Validator;
import dev.ikm.ike.esh.tinkarizer.etl.verification.NoOpVerifier;
import dev.ikm.ike.esh.tinkarizer.etl.verification.Verifier;
import dev.ikm.ike.esh.tinkarizer.event.code.EventCodeExtractor;
import dev.ikm.ike.esh.tinkarizer.event.code.EventCodeInitializer;
import dev.ikm.ike.esh.tinkarizer.event.code.EventCodeTransformer;
import dev.ikm.ike.esh.tinkarizer.event.set.EventSetExtractor;
import dev.ikm.ike.esh.tinkarizer.event.set.EventSetTransformer;
import dev.ikm.ike.esh.tinkarizer.starter.data.ESHStarterData;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "esh-tinkarizer", mixinStandardHelpOptions = true, description = "Run ESH tinkarization with required input files.")
public class App implements Callable<Integer> {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(App.class);

    @Option(names = { "-d", "--datastore" }, required = true, description = "Path to the datastore directory.")
    private Path datastore;

    @Option(names = { "-s",
            "--event-set-file" }, required = true, description = "Path to the ESH Event Set Excel file.")
    private Path eventSetFile;

    @Option(names = { "-c",
            "--event-code-file" }, required = true, description = "Path to the ESH Event Code Excel file.")
    private Path eventCodeFile;

    public static void main(String[] args) {
        System.exit(new CommandLine(new App()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        try (Database _ = new Database(datastore.toFile(), "Open SpinedArrayStore")) {
            validateDatastore(datastore);
            validateFile(eventSetFile, "event set");
            validateFile(eventCodeFile, "event code");

            // Create Pipeline Orchestrator
            PipelineOrchestrator orchestrator = new PipelineOrchestrator();
            long time = System.currentTimeMillis();

            // Create Global Index
            EntityIndex globalIndex = new EntityIndex();

            // Create Event Set Pipeline
            Pipeline eventSetPipeline = createEventSetPipeline(globalIndex, time);
            orchestrator.addPipeline(eventSetPipeline);

            // // Create Event Code Pipeline
            // Pipeline eventCodePipeline = createEventCodePipeline(globalIndex, time);
            // orchestrator.addPipeline(eventCodePipeline);

            // Run Pipelines
            orchestrator.runPipelines();
        }

        return 0;
    }

    private Pipeline createEventCodePipeline(EntityIndex globalIndex, long time) {
        Initializer initializer = new EventCodeInitializer();
        Extractor eventCodeExtractor = new EventCodeExtractor(globalIndex);
        Transformer eventCodeTransformer = new EventCodeTransformer(globalIndex);
        Validator validator = new NoOpValidator();
        Loader eventCodeLoader = new SimpleLoader();
        Verifier verification = new NoOpVerifier();

        eventCodeExtractor.addFileToExtract(eventCodeFile.toFile());

        PipelineConfig ecPipelineConfig = new PipelineConfig("Event Code Pipeline", initializer, eventCodeExtractor,
                eventCodeTransformer, validator, eventCodeLoader, verification);
        LoadConfig loadConfig = new LoadConfig(time, ESHStarterData.ESH_AUTHOR_UUID, ESHStarterData.ESH_MODULE_UUID,
                ESHStarterData.DEVELOPMENT_PATH_UUID);

        return new BatchPipeline(ecPipelineConfig, loadConfig, 1_000);
    }

    private Pipeline createEventSetPipeline(EntityIndex globalIndex, long time) {

        Initializer initializer = new NoOpInitializer();
        Extractor eventSetExtractor = new EventSetExtractor(globalIndex);
        Transformer eventSetTransformer = new EventSetTransformer();
        Validator validator = new NoOpValidator();
        Loader eventSetLoader = new SimpleLoader();
        Verifier verification = new NoOpVerifier();

        eventSetExtractor.addFileToExtract(eventSetFile.toFile());

        PipelineConfig eventSetConfig = new PipelineConfig("Event Set Composer", initializer, eventSetExtractor,
                eventSetTransformer, validator, eventSetLoader, verification);
        LoadConfig loadConfig = new LoadConfig(time, ESHStarterData.ESH_AUTHOR_UUID, ESHStarterData.ESH_MODULE_UUID,
                ESHStarterData.DEVELOPMENT_PATH_UUID);

        return new BatchPipeline(eventSetConfig, loadConfig, 1_000);
    }

    private static void validateDatastore(Path datastorePath) {
        if (!Files.exists(datastorePath)) {
            throw new CommandLine.ParameterException(new CommandLine(new App()),
                    "Datastore path does not exist: " + datastorePath);
        }
        if (!Files.isDirectory(datastorePath)) {
            throw new CommandLine.ParameterException(new CommandLine(new App()),
                    "Datastore path must be a directory: " + datastorePath);
        }
        LOG.info("Datastore: {}", datastorePath.toAbsolutePath().normalize());

    }

    private static void validateFile(Path filePath, String label) {
        if (!Files.exists(filePath)) {
            throw new CommandLine.ParameterException(new CommandLine(new App()),
                    "The " + label + " file does not exist: " + filePath);
        }
        if (!Files.isRegularFile(filePath)) {
            throw new CommandLine.ParameterException(new CommandLine(new App()),
                    "The " + label + " path must be a file: " + filePath);
        }
        LOG.info("Event Set File: {}", filePath.toAbsolutePath().normalize());
    }
}