package main.esper;

import com.google.common.collect.ImmutableList;
import main.esper.testmodel.EsperTestBolt;
import org.testng.annotations.Test;

/**
 * General tests around esper and its storm integration
 */
public class EsperBoltTest {


    @Test
    public void aggregatesCheck() {
        String statement = "select count(*) as cnt, avg(num) as avgnum, sum(num) as sumnum";
        String num = "num";

        EsperTestBolt.setup()
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
    public void pattern_followsCheck()
    {
        String statement = "select 'detected' as res from pattern [ every (_EVT(id='A') -> _EVT(id='B')) ]";

        String id = "id";
        EsperTestBolt.setup()
                .statement(statement)
                .usingFieldType(String.class)
                .withInFields(ImmutableList.of(id))
                .withOutFields(ImmutableList.of("res"))
                .init()
                .tuple().with(id, "A").push()
                .tuple().with(id, "C").push()
                .tuple().with(id, "D").push()
                .tuple().with(id, "B").pushAndWait(500)
                .checkHasEmitted()
                .checkLastMessage(new Object[]{"detected"});
    }

}
