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
                .timedTuple().pushAndWait(1000)
                .checkLastMessage(new Object[]{1L});
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
