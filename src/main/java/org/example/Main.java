package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Main {
    private static int rangeSensor = 5;
    private static int rangeSpeedMin = 50;
    private static int rangeSpeedMax = 100;

    // hohe wahrscheinlichkeit auf null werte
    private static int rangeAmountSensordata = 5;
    private static Random rand = new Random();

    public static void main(String[] args) throws InterruptedException, EPCompileException, EPDeployException {
        // http://www.esper.espertech.com/release-7.1.0/esper-reference/html/gettingstarted.html

        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(SensorData.class);
        configuration.getCommon().addEventType(FlattenedData.class);
        EPCompiler compiler = EPCompilerProvider.getCompiler();

        CompilerArguments arguments = new CompilerArguments(configuration);
        //String q1 = "select istream id, speed, eventTimestamp from SensorData";
        String q1 = "@name('RetrieveMeasurements') select id, speed, timestamp from SensorData;";
        String q2 = "@name('FilterMeasurements') select * from FlattenedData;";
        EPCompiled epCompiled = compiler.compile(q1 + q2, arguments);

        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        runtime.initialize();
        EPDeployment deployment = runtime.getDeploymentService().deploy(epCompiled);

        EPStatement retrieveStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "RetrieveMeasurements");
        retrieveStatement.addListener((newData, oldData, statement, runtime1) -> {
            //retrieve generated Data
            int id = (int) newData[0].get("id");
            List<Double> speed = (List<Double>) newData[0].get("speed");
            long eventTimestamp = (long) newData[0].get("timestamp");

            //flatten Data -> maybe helpful for later use
            List<FlattenedData> flattenedData = new ArrayList<>();
            speed.forEach(s -> flattenedData.add(new FlattenedData(id, s, eventTimestamp)));
            flattenedData.forEach(data -> System.out.println(data.getId() + " " + Instant.ofEpochMilli(data.getTimestamp()) + " " + data.getSpeed()));
            flattenedData.forEach(data -> runtime1.getEventService().sendEventBean(data, "FlattenedData"));
        });

        EPStatement filterStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "FilterMeasurements");
        retrieveStatement.addListener((newData, oldData, statement, runtime2) -> {
            int id = (int) newData[0].get("id");
            List<Double> speed = (List<Double>) newData[0].get("speed");
            long eventTimestamp = (long) newData[0].get("timestamp");
            //TODO: Filter out empty and negative Values
        });

        while (true) {
            Thread.sleep(300);

            int currentSensor = rand.nextInt(rangeSensor);
            int numSpeedValues = rand.nextInt(rangeAmountSensordata);
            List<Double> speedValues = new ArrayList<>();

            for (int i = 0; i < numSpeedValues; i++) {
                double currentSpeed = (double) (rand.nextInt((rangeSpeedMax - rangeSpeedMin) + 1) + rangeSpeedMin);
                speedValues.add(currentSpeed);
            }

            // https://esper.espertech.com/release-7.0.0/esper-reference/html/configuration.html#config-engine-time-source
            // eigentlich sollte es auch per default gehen, aber ich bekomme den wert nicht abgefragt
            // todo: 
            long timestamp = System.currentTimeMillis();

            runtime.getEventService().sendEventBean(new SensorData(currentSensor, speedValues, timestamp), "SensorData");
        }

    }
}