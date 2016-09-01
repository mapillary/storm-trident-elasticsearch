package com.github.fhuss.storm.elasticsearch;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
//import com.github.tlrx.elasticsearch.test.EsSetup;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Paths;

//import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;

/**
 * Default class for starting/stopping storm local cluster before and after tests.
 *
 * @author fhussonnois
 */
public abstract class BaseLocalClusterTest {

    public static final Settings SETTINGS = Settings.settingsBuilder().loadFromPath(Paths.get("elasticsearch.yml")).build();

//    protected EsSetup esSetup;
    protected LocalCluster cluster;
    protected LocalDRPC drpc;
    protected Settings settings;
    protected String index;

    /**
     * Creates a new {@link BaseLocalClusterTest}.
     * @param index name of the index.
     */
    public BaseLocalClusterTest(String index) {
        this(index, SETTINGS);
    }

    /**
     * Creates a new {@link BaseLocalClusterTest}.
     * @param index name of the index.
     * @param settings settings
     */
    public BaseLocalClusterTest(String index, Settings settings) {
        this.index = index;
        this.settings = settings;
    }

    public ClientFactory.LocalTransport getLocalClient() {
        return new ClientFactory.LocalTransport(settings.getAsMap());
    }

    @Before
    public void setUp() {
//        esSetup = new EsSetup(settings);
//        esSetup.execute(createIndex(index));

        drpc = new LocalDRPC();
        StormTopology topology = buildTopology();

        cluster = new LocalCluster();
        cluster.submitTopology("elastic-storm", new Config(), topology);

        Utils.sleep(10000); // let's do some work
    }

    @After
    public void tearDown() {
        drpc.shutdown();
        cluster.shutdown();
//        esSetup.terminate();
    }

    /**
     * Builds the topology that must be submitted to the local cluster.
     */
    protected abstract StormTopology buildTopology();
}
