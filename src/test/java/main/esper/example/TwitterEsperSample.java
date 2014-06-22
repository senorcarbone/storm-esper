package main.esper.example;

import main.esper.RCEsperBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TwitterEsperSample
{
    public static void main(String[] args)
    {
        final String username = args[0];
        final String pwd = args[1];

        TopologyBuilder builder = new TopologyBuilder();
        TwitterSpout spout = new TwitterSpout(username, pwd);
        RCEsperBolt bolt = new RCEsperBolt.Builder()
                                      .inputs().aliasComponent("spout1").toEventType("Tweets")
                                      .outputs().onDefaultStream().emit("tps", "maxRetweets")
                                      .statements().add("select count(*) as tps, max(retweetCount) as maxRetweets from Tweets.win:time_batch(1 sec)")
                                      .build();

        builder.setSpout("spout1", spout);
        builder.setBolt("bolt1", bolt).shuffleGrouping("spout1");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}
