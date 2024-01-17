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

    // hohe wahrscheinlichkeit auf null werte
    private static int rangeAmountSensordata = 5;
    private static Random rand = new Random();
    private static String seq = "1, 2, 3";

    public static void main(String[] args) throws InterruptedException, EPCompileException, EPDeployException {
        // http://www.esper.espertech.com/release-7.1.0/esper-reference/html/gettingstarted.html

        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(SensorData.class);
        configuration.getCommon().addEventType(FlattenedData.class);
        EPCompiler compiler = EPCompilerProvider.getCompiler();

        CompilerArguments arguments = new CompilerArguments(configuration);
        //String q1 = "select istream id, speed, eventTimestamp from SensorData";
        String q1 = "@name('RetrieveMeasurements') select id, speed, timestamp from SensorData;";
        String q2 = "@Name('AvgByKey') select id, avg(speed) as averageSpeed from FlattenedData#time_batch(5 sec) group by id;";
        String q3 = "@Name('AvgOverall') select avg(speed) as overallAverageSpeed from FlattenedData#time_batch(5 sec) where id in (" + seq + ")";

        EPCompiled epCompiled = compiler.compile(q1 + q2 + q3, arguments);

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        runtime.initialize();
        EPDeployment deployment = runtime.getDeploymentService().deploy(epCompiled);

        EPStatement retrieveStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "RetrieveMeasurements");
        retrieveStatement.addListener((newData, oldData, statement, runtime1) -> {
            //retrieve generated Data
            int id = (int) newData[0].get("id");
            List<Double> speed = (List<Double>) newData[0].get("speed");
            long timestamp = (long) newData[0].get("timestamp");

            //Filter the negative out
            speed.removeIf(value -> value <= 0.0);
            speed.removeIf(Objects::isNull);
            //convert to km/h
            speed.replaceAll(s -> s * 3.6);
            if (!speed.isEmpty()) {
                System.out.println(timestamp + " " + id + " " + speed);
                //flatten Data
                List<FlattenedData> flattenedData = new ArrayList<>();
                speed.forEach(s -> flattenedData.add(new FlattenedData(id, s, timestamp)));
                flattenedData.forEach(data -> runtime1.getEventService().sendEventBean(data, "FlattenedData"));
            }
        });

        //prints avg for each sensor
        EPStatement avgStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "AvgByKey");
        avgStatement.addListener((newData, oldData, statement, runtime2) -> {
            for (EventBean eventBean : newData) {
                int id = (int) eventBean.get("id");
                if (eventBean.get("averageSpeed") != null) {
                    double averageSpeed = (double) eventBean.get("averageSpeed");
                    System.out.println("Average speed for ID " + id + " is " + String.format(Locale.US, "%.2f km/h", averageSpeed));
                } else
                    System.out.println("There were no measurements for " + id + " in this window");
            }
        });

        //prints avg for sensor sequence
        EPStatement avgSeqStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "AvgOverall");
        avgSeqStatement.addListener((newData, oldData, statement, runtime3) -> {
            if (newData != null && newData.length > 0) {
                double averageSpeed = (double) newData[0].get("overallAverageSpeed");
                System.out.println("Average speed over sequence is " + String.format(Locale.US, "%.2f km/h", averageSpeed));
            }
        });

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
            // todo: 
            long timestamp = System.currentTimeMillis();

            runtime.getEventService().sendEventBean(new SensorData(currentSensor, speedValues, timestamp), "SensorData");
        }

    }
}