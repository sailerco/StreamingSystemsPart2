package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.util.*;


public class Main {
    private static int rangeSensor = 5;
    private static int minSpeed = 10;
    private static int maxSpeed = 30;
    private static int rangeAmountSensordata = 5;
    private static Random rand = new Random();
    private static String seq = "1, 2, 3";

    public static void main(String[] args) throws InterruptedException, EPCompileException, EPDeployException {
        // http://www.esper.espertech.com/release-7.1.0/esper-reference/html/gettingstarted.html

        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(SensorData.class);
        configuration.getCommon().addEventType(FlattenedData.class);
        configuration.getCommon().addEventType(AggregatedData.class);
        configuration.getCommon().addEventType(TrafficJam.class);
        EPCompiler compiler = EPCompilerProvider.getCompiler();

        CompilerArguments arguments = new CompilerArguments(configuration);
        String q1 = "@name('RetrieveMeasurements') select id, speed, timestamp from SensorData;";
        String q2 = "@name('AvgByKey') select id, avg(speed) as averageSpeed from FlattenedData#ext_timed_batch(timestamp, 5 sec) group by id;";
        String q3 = "@name('AvgOverall') select avg(speed) as overallAverageSpeed " +
                "from FlattenedData#ext_timed_batch(timestamp, 5 sec) where id in (" + seq + ");";
        String q4 = "insert into AggregatedData select id as id, avg(speed) as speed, min(timestamp) as firstTimestamp, max(timestamp) as lastTimestamp " +
                "from FlattenedData#ext_timed_batch(timestamp, 5 sec) group by id having count(speed) > 0;";
        String q5 = "@name('CheckForTrafficJam') select id, min(speed) as minSpeed, max(speed) as maxSpeed, min(firstTimestamp) as firstTimestamp, max(lastTimestamp) as lastTimestamp " +
                "from AggregatedData#ext_timed_batch(firstTimestamp, 10 sec) group by id having min(speed) < max(speed) * 0.8";

        EPCompiled epCompiled = compiler.compile(q1 + q2 + q3 + q4 + q5, arguments);

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        runtime.initialize();
        EPDeployment deployment = runtime.getDeploymentService().deploy(epCompiled);

        EPStatement retrieveStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "RetrieveMeasurements");
        retrieveStatement.addListener(new Listener.SensorEventListener());

        //prints avg for each sensor
        EPStatement avgStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "AvgByKey");
        avgStatement.addListener(new Listener.AvgEventListener());

        //prints avg for sensor sequence
        EPStatement avgSeqStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "AvgOverall");
        avgSeqStatement.addListener((newData, oldData, statement, runtime3) -> {
            if (newData != null && newData.length > 0) {
                double averageSpeed = (double) newData[0].get("overallAverageSpeed");
                System.out.println("Average speed over sequence is " + String.format(Locale.US, "%.2f km/h", averageSpeed));
            }
        });

        // Traffic Jam
        EPStatement checkTrafficJam = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "CheckForTrafficJam");
        checkTrafficJam.addListener(new Listener.TrafficEventListener());

        while (true) {
            Thread.sleep(300);

            int currentSensor = rand.nextInt(rangeSensor);
            int numSpeedValues = rand.nextInt(rangeAmountSensordata);
            List<Double> speedValues = new ArrayList<>();

            for (int i = 0; i < numSpeedValues; i++) {
                double s;
                if (rand.nextDouble() < 0.1) //if random number is smaller than 10%, for testing purpose
                    s = maxSpeed * -rand.nextDouble(); // Generate a random negative value
                else
                    s = minSpeed + (maxSpeed - minSpeed) * rand.nextDouble(); // Generate value between max and min
                speedValues.add(s);
            }

            // https://esper.espertech.com/release-7.0.0/esper-reference/html/configuration.html#config-engine-time-source
            // eigentlich sollte es auch per default gehen, aber ich bekomme den wert nicht abgefragt
            long timestamp = System.currentTimeMillis();

            runtime.getEventService().sendEventBean(new SensorData(currentSensor, speedValues, timestamp), "SensorData");
        }

    }

}
