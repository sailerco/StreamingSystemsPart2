package org.example;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;


public class Main {

    public static void main(String[] args) throws InterruptedException {
        // http://www.esper.espertech.com/release-7.1.0/esper-reference/html/gettingstarted.html

        EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();

        engine.getEPAdministrator().getConfiguration().addEventType(SensorData.class);

        String epl = "select id, speed from SensorData";
        EPStatement statement = engine.getEPAdministrator().createEPL(epl);

        statement.addListener( (newData, oldData) -> {
            int id = (int) newData[0].get("id");
            double speed = (double) newData[0].get("speed");
            System.out.println(String.format("Id: %d, Speed: %.2f", id, speed));
        });

        engine.getEPRuntime().sendEvent(new SensorData(2, 34));


        /*
        while (true) {
            String data = TestGenerator.generateTestData();
            Thread.sleep(100);
            System.out.println(data);
        }*/

    }
}