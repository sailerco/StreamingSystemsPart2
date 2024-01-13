package org.example;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;


public class Main {
    private static int rangeSensor = 5;
    private static int rangeSpeedMin = -30;
    private static int rangeSpeedMax = 230;

    // hohe wahrscheinlichkeit auf null werte
    private static int rangeAmountSensordata = 5;
    private static Random rand = new Random();

    public static void main(String[] args) throws InterruptedException {
        // http://www.esper.espertech.com/release-7.1.0/esper-reference/html/gettingstarted.html

        //todo: auf Folie 319 und 320 macht er irgendwie was anderes. Idk meinst ist aus Esper Beispiel
        // getCommon wird bei mir auch nicht gefunden

        EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();

        engine.getEPAdministrator().getConfiguration().addEventType(SensorData.class);

        String getSensorData = "select istream id, speed, eventTimestamp from SensorData";


        EPStatement statement = engine.getEPAdministrator().createEPL(getSensorData);

        statement.addListener((newData, oldData) -> {

            int id = (int) newData[0].get("id");
            List<Double> speed = (List<Double>) newData[0].get("speed");
            long eventTimestamp = (long) newData[0].get("eventTimestamp");

            Instant instant = Instant.ofEpochMilli(eventTimestamp);
            LocalDateTime time = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < speed.size(); i++) {
                sb.append(String.format(Locale.ENGLISH, "%.1f", speed.get(i))); // Verwenden Sie Locale.ENGLISH, um den Punkt als Dezimaltrennzeichen zu erzwingen
                if (i < speed.size() - 1) {
                    sb.append(",");
                }
            }
            System.out.println(String.format("Event Timestamp: %s, Id: %d, Speeds: %s", LocalDateTime.ofEpochSecond(eventTimestamp/1000, 0, ZoneOffset.UTC), id, sb.toString() ));
        });


        while (true) {
            Thread.sleep(300);

            int currentSensor = rand.nextInt(rangeSensor);
            int numSpeedValues = rand.nextInt(rangeAmountSensordata);
            List<Double> speedValues = new ArrayList<>();

            for (int i = 0; i < numSpeedValues; i++) {
                double currentSpeed = (double) (rand.nextInt((rangeSpeedMax-rangeSpeedMin) + 1) + rangeSpeedMin);
                speedValues.add(currentSpeed);
            }

            // https://esper.espertech.com/release-7.0.0/esper-reference/html/configuration.html#config-engine-time-source
            // eigentlich sollte es auch per default gehen, aber ich bekomme den wert nicht abgefragt
            // todo: 
            long eventTimestamp = System.currentTimeMillis();

            engine.getEPRuntime().sendEvent(new SensorData(currentSensor, speedValues, eventTimestamp));
        }

    }
}