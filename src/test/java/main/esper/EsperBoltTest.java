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
                .checkLastMessage(new Object[]{2L, 15.0, 30});
    }

}
