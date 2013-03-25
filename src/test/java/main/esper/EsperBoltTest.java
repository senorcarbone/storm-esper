package main.esper;

import com.google.common.collect.ImmutableList;
import main.esper.testmodel.EsperBoltDummy;
import org.testng.annotations.Test;

/**
 * General tests around esper and its storm integration
 */
public class EsperBoltTest {


    @Test
    public void aggregatesCheck() {
        String statement = "select count(*) as cnt, avg(num) as avgnum, sum(num) as sumnum";
        String num = "num";

        EsperBoltDummy.setup()
                .statement(statement)
                .withinBatchWindow(1)
                .usingFieldType(Integer.class)
                .withInFields(ImmutableList.of(num))
                .withOutFields(ImmutableList.of("cnt", "avgnum", "sumnum"))
                .init()
                .tuple().with(num, 10).push()
                .tuple().with(num, 20).pushAndWait(1200)
                .checkHasEmitted()
                .checkLastMessage(new Object[]{2L, 15.0, 30});
    }

    @Test
    public void views_ext_timedCheck() {
        String statement = "select count(*) as cnt from _EVT.win:ext_timed(timestamp, 1 seconds)";
        EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(Long.class)
                .withInFields(ImmutableList.of("timestamp"))
                .withOutFields(ImmutableList.of("cnt"))
                .init()
                .timedTuple().pushAndWait(1000)
                .timedTuple().push()
                .timedTuple().pushAndWait(1100)
                .checkLastMessage(new Object[]{2L})
                .timedTuple().push()
                .checkLastMessage(new Object[]{1L});
    }

    @Test
    public void views_ext_timedCheck2() {
        String statement = "select count(*) as cnt from _EVT.win:ext_timed(timestamp, 30 milliseconds)";
        EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(Long.class)
                .withInFields(ImmutableList.of("timestamp"))
                .withOutFields(ImmutableList.of("cnt"))
                .init()
                .tuple().with("timestamp", 10).push()
                .tuple().with("timestamp", 20).push()
                .tuple().with("timestamp", 30).push()
                .tuple().with("timestamp", 40).push()
                .tuple().with("timestamp", 45).push()
                .tuple().with("timestamp", 50).push()
                .checkLastMessage(new Object[]{4L});
    }

    @Test
    public void views_ext_timed_CanBeDeterministic() {
        String statement = "select avg(temp) as tempavg from _EVT.win:length_batch(3)";
        EsperBoltDummy.setup()
                .statement(statement)
                .withInFields(ImmutableList.of("temp"))
                .withOutFields(ImmutableList.of("tempavg"))
                .init()
                .tuple().with("temp", 10).push()
                .tuple().with("temp", 20).push()
                .checkEmitSize(0)
                .tuple().with("temp", 30).pushAndWait(200)
                .checkEmitSize(1)
                .checkLastMessage(new Object[]{20.0});
    }

    @Test
    public void pattern_every_followsCheck() {
        String statement = "select 'detected' as res from pattern [ every (_EVT(id='A') -> _EVT(id='B'))]";
        testPatternOrderingBolt(statement, 2);
    }

    @Test
    public void pattern_EveryToAny_followsCheck() {
        String statement = "select 'detected' as res from pattern [ (every _EVT(id='A') -> _EVT(id='B'))]";
        testPatternOrderingBolt(statement, 3);
    }

    @Test
    public void pattern_AnyToEvery_followsCheck() {
        String statement = "select 'detected' as res from pattern [ ( _EVT(id='A') -> every _EVT(id='B'))]";
        testPatternOrderingBolt(statement, 3);
    }

    @Test
    public void pattern_EveryToEvery_followsCheck() {
        String statement = "select 'detected' as res from pattern [ (every _EVT(id='A') -> every _EVT(id='B'))]";
        testPatternOrderingBolt(statement, 7);
    }

    private EsperBoltDummy testPatternOrderingBolt(String statement, int countMatch) {
        String id = "id";
        return EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(String.class)
                .withInFields(ImmutableList.of(id))
                .withOutFields(ImmutableList.of("res"))
                .init()
                .tuple().with(id, "A").push()
                .tuple().with(id, "B").push()
                .tuple().with(id, "A").push()
                .tuple().with(id, "A").push()
                .tuple().with(id, "B").push()
                .tuple().with(id, "B").pushAndWait(500)
                .checkEmitSize(countMatch)
                .checkLastMessage(new Object[]{"detected"});
    }
}
