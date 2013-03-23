package main.esper;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * General tests around esper and its storm integration
 */
public class EsperBoltTest {

    public static final String TESTSPOUT = "testspout";
    public static final String NUMBER = "number";
    public static final String TEST_EVENT = "TestEvent";
    private TopologyContext topologyContext = mock(TopologyContext.class);

    @Test
    public void PrimitiveTypesCheck() {
        String statement = "select count(*) as cnt, avg(num) as avgnum, sum(num) as sumnum from TestEvent.win:time_batch(2 sec)";

        EsperBolt testBolt = new EsperBolt.Builder()
                .inputs().aliasComponent(TESTSPOUT).withField(NUMBER).ofType(Integer.class).toEventType(TEST_EVENT)
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

        testBolt.prepare(Maps.newHashMap(), topologyContext, outputCollector);
        Map<String, Object> data = Maps.newHashMap();
        data.put(NUMBER, new Integer(10));
        testBolt.execute(new TestTuple(TESTSPOUT, TEST_EVENT, data));
    }


    public class TestTuple extends TupleImpl {

        private final String src;
        private final String streamId;
        private final List<String> keys = Lists.newArrayList();
        private final List<Object> values = Lists.newArrayList();

        public TestTuple(String src, String streamId, Map<String, Object> data) {
            super(null, null, 0, null);
            this.src = src;
            this.streamId = streamId;
            for (Entry<String, Object> entry : data.entrySet()) {
                keys.add(entry.getKey());
                values.add(entry.getValue());
            }
        }

        @Override
        public String getSourceComponent() {
            return src;
        }

        @Override
        public String getSourceStreamId() {
            return streamId;
        }

        @Override
        public Fields getFields() {
            return new Fields(keys);
        }

        @Override
        public Object getValue(int index) {
            return values.get(index);
        }
    }
}
