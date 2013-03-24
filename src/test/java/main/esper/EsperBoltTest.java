package main.esper;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * General tests around esper and its storm integration
 */
public class EsperBoltTest {

    public static final String TEST_SPOUT = "testspout";
    public static final String NUMBER = "num";
    public static final String TEST_EVENT = "TestEvt";
    public static final String STREAMID = "AStream";
    private TopologyContext topologyContext = mock(TopologyContext.class);

    @Test
    public void PrimitiveTypesCheck() {
        String statement = "select count(*) as cnt, avg(num) as avgnum, sum(num) as sumnum from TestEvt.win:time_batch(2 sec)";

        EsperBolt testBolt = new EsperBolt.Builder()
                .inputs().aliasStream(TEST_SPOUT, STREAMID).withField(NUMBER).ofType(Integer.class).toEventType(TEST_EVENT)
                .outputs().onDefaultStream().emit("cnt", "avgnum", "sumnum")
                .statements().add(statement)
                .build();

        OutputCollector outputCollector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String s, Collection<Tuple> tuples, List<Object> objects) {
                System.err.println("Got from " + s + " tuples: " + tuples + " objects: " + objects);
                return Collections.EMPTY_LIST;
            }

            @Override
            public void emitDirect(int i, String s, Collection<Tuple> tuples, List<Object> objects) {
            }

            @Override
            public void ack(Tuple tuple) {
            }

            @Override
            public void fail(Tuple tuple) {
            }

            @Override
            public void reportError(Throwable throwable) {
            }
        });

        Map<String, Object> data = Maps.newHashMap();
        data.put(NUMBER, new Integer(10));
        DummyTuple tuple = new DummyTuple(TEST_SPOUT, STREAMID, data);
        GlobalStreamId glStrId = new GlobalStreamId(TEST_SPOUT, STREAMID);
        Map<GlobalStreamId, Grouping> retMap = ImmutableMap.of(glStrId, new Grouping());
        when(topologyContext.getThisSources()).thenReturn(retMap);
        when(topologyContext.getComponentOutputFields(anyString(), anyString())).thenReturn(tuple.getFields());
        testBolt.prepare(Maps.newHashMap(), topologyContext, outputCollector);
        testBolt.execute(tuple);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
