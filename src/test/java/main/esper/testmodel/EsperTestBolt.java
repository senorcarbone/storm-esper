package main.esper.testmodel;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import main.esper.DummyTuple;
import main.esper.EsperBolt;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsperTestBolt {

    private static final String TEST_SPOUT = "testspout";
    private static final String TEST_EVENT = "TestEvt";
    private static final String STREAMID = "TestStream";
    private List<List<Object>> emitted = Lists.newArrayList();
    private EsperBolt testBolt;

    private EsperTestBolt() {
    }

    public static SimpleEsperSetup setup() {
        return new EsperTestBolt().build();
    }

    private SimpleEsperSetup build() {
        return new SimpleEsperSetup();
    }

    public SimpleTupleSetup tuple() {
        return new SimpleTupleSetup();
    }

    public EsperTestBolt checkLastMessage(Object[] vals) {
        assertThat(emitted.get(emitted.size()-1), contains(vals));
        return this;
    }

    public class SimpleEsperSetup {
        private TopologyContext topologyContext = mock(TopologyContext.class);
        private List<String> inputFields = Lists.newArrayList();
        private List<String> outputFields = Lists.newArrayList();
        private Class inTypes = Integer.class;
        private String window;
        private String statement;

        private OutputCollector outputCollector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String s, Collection<Tuple> tuples, List<Object> objects) {
                emitted.add(objects);
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

        public SimpleEsperSetup usingFieldType(Class inFieldClass) {
            this.inTypes = inFieldClass;
            return this;
        }

        public SimpleEsperSetup withInFields(List<String> inFields) {
            this.inputFields = inFields;
            return this;
        }

        public SimpleEsperSetup withOutFields(List<String> outFields) {
            this.outputFields = outFields;
            return this;
        }

        public SimpleEsperSetup statement(String stmt) {
            this.statement = stmt;
            return this;
        }

        public SimpleEsperSetup withinBatchWindow(int sec) {
            window = ".win:time_batch(" + sec + " sec)";
            return this;
        }

        public SimpleEsperSetup withinWindow(int sec) {
            window = ".win:time(" + sec + " sec)";
            return this;
        }

        public EsperTestBolt init() {
            build();
            return EsperTestBolt.this;
        }

        private void build() {
            testBolt = new EsperBolt.Builder()
                    .inputs().aliasStream(TEST_SPOUT, STREAMID).withFields(inputFields.toArray(new String[inputFields.size()])).ofType(inTypes).toEventType(TEST_EVENT)
                    .outputs().onDefaultStream().emit(outputFields.toArray(new String[outputFields.size()]))
                    .statements().add(statement + " from " + TEST_EVENT + window)
                    .build();
            GlobalStreamId glStrId = new GlobalStreamId(TEST_SPOUT, STREAMID);
            Map<GlobalStreamId, Grouping> retMap = ImmutableMap.of(glStrId, new Grouping());
            when(topologyContext.getThisSources()).thenReturn(retMap);
            when(topologyContext.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields(inputFields));
            testBolt.prepare(Maps.newHashMap(), topologyContext, outputCollector);
        }
    }

    public class SimpleTupleSetup {

        private Map<String, Object> data = Maps.newHashMap();

        public SimpleTupleSetup with(String fieldName, Object val) {
            data.put(fieldName, val);
            return this;
        }

        public SimpleTupleSetup with(Map<String, Object> mappings) {
            data.putAll(mappings);
            return this;
        }

        public EsperTestBolt push() {
            EsperTestBolt.this.testBolt.execute(new DummyTuple(TEST_SPOUT, STREAMID, data));
            return EsperTestBolt.this;
        }

        public EsperTestBolt pushAndWait(long millis) {
            push();
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return EsperTestBolt.this;
        }
    }
}