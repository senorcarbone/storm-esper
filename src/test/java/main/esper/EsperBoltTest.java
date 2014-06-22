package main.esper;

import com.google.common.collect.ImmutableList;
import main.esper.testmodel.EsperBoltDummy;
import org.testng.annotations.Test;

/**
 * General tests around esper and its storm integration
 */
public class EsperBoltTest {

    public static final long SMOKE_DETECTOR = 1;
    public static final long TEMP_SENSOR = 2;
    public static final int BUILDING_1 = 23222;
    public static final int BUILDING_2 = 123;
    public static final long SENSOR = 0;

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
    public void testAvg() {
        String statement = "select count(*) as cnt from _EVT.std:groupwin(part).win:ext_timed(timestamp, 1 seconds)";
        EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(Double.class)
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
    public void views_ext_timedCheck() {
        String statement = "select part, avg(num) as aver from _EVT.win:ext_timed(Timestamp, 1 seconds)";
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
    public void views_ext_timed_aggregates() {
        String statement = "select avg(A.temp) as tempavg, max(A.ts) as ts  from _EVT.win:ext_timed(ts, 2 seconds) as A";
        EsperBoltDummy.setup()
                .statement(statement)
                .withInFields(ImmutableList.of("temp", "ts"))
                .withOutFields(ImmutableList.of("tempavg", "ts"))
                .init()
                .tuple().with("temp", 10).with("ts", 1000).push()
                .tuple().with("temp", 20).with("ts", 1200).push()
                .tuple().with("temp", 30).with("ts", 1500).pushAndWait(200)
                .checkLastMessage(new Object[]{20.0, 1500})
                .tuple().with("temp", 50).with("ts", 3300).pushAndWait(200)
                .checkLastMessage(new Object[]{40.0, 3300});
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
    public void jointstreams_propertyInference() {
        String statement = "select 'detected' as res from pattern [every (smoke=_EVT(type=1) -> tp=_EVT(type=2, temp > 80))] where smoke.area = tp.area";
        EsperBoltDummy.setup()
                .statement(statement)
                .withInFields(ImmutableList.of("type", "temp", "area"))
                .withOutFields(ImmutableList.of("res"))
                .init()
                .tuple().with("type", SMOKE_DETECTOR).with("area", BUILDING_1).push()
                .tuple().with("type", SMOKE_DETECTOR).with("area", 0).push()
                .tuple().with("type", TEMP_SENSOR).with("temp", 90).with("area", BUILDING_1).pushAndWait(100)
                .checkHasEmitted()
                .checkLastMessage(new Object[]{"detected"});
    }

    @Test
    public void jointstreams_propertyInference2() {
        //.win:ext_timed(smoke.timestamp, 10 milliseconds)
        String patternStatement = "select 'detected' as res from pattern [smoke=_EVT(type=1) -> tp=_EVT(type=2, temp > 80)].win:length(1) where smoke.area = tp.area";

        EsperBoltDummy.setup()
                .statement(patternStatement)
                .usingFieldType(Long.class)
                .withInFields(ImmutableList.of("type", "temp", "area", "timestamp"))
                .withOutFields(ImmutableList.of("res"))
                .init()
                .tuple().with("type", SMOKE_DETECTOR).with("area", BUILDING_1).with("timestamp", 5l).push()
                .tuple().with("type", SMOKE_DETECTOR).with("area", 0l).with("timestamp", 40l).push()
                .tuple().with("type", SMOKE_DETECTOR).with("area", 0l).with("timestamp", 45l).push()
                .tuple().with("type", TEMP_SENSOR).with("temp", 90l).with("area", BUILDING_1).with("timestamp", 50l).pushAndWait(50)
                .checkNoEmittions()
                .tuple().with("type", SMOKE_DETECTOR).with("area", BUILDING_1).with("timestamp", 80l).push()
                .tuple().with("type", SMOKE_DETECTOR).with("area", BUILDING_1).with("timestamp", 140l).push()
                .tuple().with("type", TEMP_SENSOR).with("temp", 90l).with("area", BUILDING_1).with("timestamp", 145l).pushAndWait(100)
                .checkEmitSize(1);
    }

    /*
                    "select 'SIP_FLOODING_ALERT' as flag, target, intruder, count(*) as alertLevel, Timestamp \n" +
                        "from _EVT(Request_Line = 'INVITE SIP').std:groupwin(to).win:ext_timed(Timestamp, 10 seconds) \n" +
                        "   match_recognize ( \n" +
                        "   partition by To \n" +
                        "   measures B.To as target, B.Frm as intruder, B.Timestamp as Timestamp \n" +
                        "   pattern (A B) \n" +
                        "   define \n" +
                        "   B as B.To = A.To and B.Timestamp - A.Timestamp < 2000 \n" +
                        "   ) group by Frm,To";
     */

    @Test
    public void testScenarioTelecom() {
        String statement =
                "select 'SIP_FLOODING_ALERT' as flag, Frm, To, count(*) as alertLevel, Timestamp \n" +
                        "from _EVT(Request_Line = 1).std:groupwin(Frm,To).win:ext_timed(Timestamp, 10 seconds) \n" +
                        "   match_recognize ( \n" +
                        "   partition by Frm,To \n" +
                        "   measures B.To as To, B.Frm as Frm, B.Timestamp as Timestamp \n" +
                        "   pattern (A B) \n" +
                        "   define \n" +
                        "   B as B.Timestamp - A.Timestamp < 2000 \n" +
                        "   ) group by Frm,To";

        EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(Long.class)
                .withInFields(ImmutableList.of("Request_Line", "Frm", "To", "Timestamp"))
                .withOutFields(ImmutableList.of("flag", "Frm", "To", "alertLevel", "Timestamp"))
                .init()
                .tuple().with("Request_Line", 1l).with("Frm", 100).with("To", 101).with("Timestamp", 0l).push()
                .tuple().with("Request_Line", 1l).with("Frm", 200).with("To", 101).with("Timestamp", 100l).pushAndWait(200)
                .checkNoEmittions()
                .tuple().with("Request_Line", 1l).with("Frm", 200).with("To", 101).with("Timestamp", 10000l).pushAndWait(200)
                .checkNoEmittions()
                .tuple().with("Request_Line", 1l).with("Frm", 100).with("To", 101).with("Timestamp", 1000l).pushAndWait(200)
                .checkHasEmitted()
                .tuple().with("Request_Line", 1l).with("Frm", 100).with("To", 101).with("Timestamp", 1500l).push()
                .tuple().with("Request_Line", 1l).with("Frm", 100).with("To", 101).with("Timestamp", 2500l).push()
                .tuple().with("Request_Line", 1l).with("Frm", 100).with("To", 101).with("Timestamp", 4500l).pushAndWait(200)
                .checkLastMessage(new Object[]{"SIP_FLOODING_ALERT", 100, 101, 2L, 2500L});

    }

    @Test
    public void match_recognize_check() {
        String statement =
                "select 'fire!' as res, area, MAX(temp) as temp, ts from _EVT(id=0).std:groupwin(area).win:ext_timed(ts, 1 seconds) \n" +
                        "  match_recognize ( \n" +
                        "  partition by area \n" +
                        "  measures B.area as area, B.temp as temp, B.ts as ts \n" +
                        "  pattern (A B) \n" +
                        "  define \n" +
                        "  A as A.type=1, \n" +
                        "  B as B.type=2 and  B.temp > 80 \n" +
                        ") group by area";

        EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(Long.class)
                .withInFields(ImmutableList.of("id", "type", "temp", "area", "ts"))
                .withOutFields(ImmutableList.of("res", "area", "temp", "ts"))
                .init()
                .tuple().with("id", SENSOR).with("type", SMOKE_DETECTOR).with("area", BUILDING_1).with("ts", 1000).push()
                .tuple().with("id", SENSOR).with("type", SMOKE_DETECTOR).with("area", BUILDING_2).with("ts", 1500).push()
                .tuple().with("id", SENSOR).with("type", TEMP_SENSOR).with("area", BUILDING_1).with("temp", 100).with("ts", 2100).push()
                .checkNoEmittions()
                .tuple().with("id", SENSOR).with("type", SMOKE_DETECTOR).with("area", BUILDING_1).with("ts", 2500).push()
                .tuple().with("id", SENSOR).with("type", TEMP_SENSOR).with("area", BUILDING_1).with("temp", 150).with("ts", 3000).pushAndWait(200)
                .tuple().with("id", SENSOR).with("type", TEMP_SENSOR).with("area", BUILDING_1).with("temp", 120).with("ts", 3200).pushAndWait(200)
                .checkEmitSize(1)
                .checkLastMessage(new Object[]{"fire!", BUILDING_1, 150, 3000})
                .tuple().with("id", SENSOR).with("type", SMOKE_DETECTOR).with("area", BUILDING_1).with("ts", 3300).push()
                .tuple().with("id", SENSOR).with("type", TEMP_SENSOR).with("area", BUILDING_1).with("temp", 120).with("ts", 3400).pushAndWait(200)
                .checkEmitSize(2)
                .checkLastMessage(new Object[]{"fire!", BUILDING_1, 150, 3400})
                .tuple().with("id", SENSOR).with("type", TEMP_SENSOR).with("area", BUILDING_2).with("temp", 100).with("ts", 2000).pushAndWait(200)
                .checkEmitSize(3)
                .checkLastMessage(new Object[]{"fire!", BUILDING_2, 100, 2000})
        ;
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

    @Test
    public void testPatternWithWindow() {
        String id = "id";
        String statement = "select 'detected' as res from pattern [ (every _EVT(id='A') ->  _EVT(id='B'))]";
        EsperBoltDummy.setup()
                .statement(statement)
                .usingFieldType(String.class)
                .withInFields(ImmutableList.of(id))
                .withOutFields(ImmutableList.of("res"))
                .init()
                .tuple().with(id, "A").push()
                .tuple().with(id, "B").pushAndWait(10)
                .tuple().with(id, "A").push()
                .tuple().with(id, "A").push()
                .tuple().with(id, "B").pushAndWait(10)
                .tuple().with(id, "B").pushAndWait(10)
                .checkEmitSize(2)
                .checkLastMessage(new Object[]{"detected"});
    }
}
