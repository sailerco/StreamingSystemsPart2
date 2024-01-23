package org.example;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.scopetest.EPAssertionUtil;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import com.espertech.esper.runtime.client.scopetest.SupportUpdateListener;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ListenerTest {
    static Configuration configuration;
    static CompilerArguments compilerArguments;
    static EPRuntime runtime;
    @BeforeAll
    public static void setup(){
        configuration = new Configuration();
        configuration.getCommon().addEventType(SensorData.class);
        configuration.getCommon().addEventType(FlattenedData.class);
        compilerArguments = new CompilerArguments(configuration);
        runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
    }
    public SupportUpdateListener setStatement(String statement) throws EPDeployException, EPCompileException {
        EPCompiled compiled = EPCompilerProvider.getCompiler().compile(statement, compilerArguments);
        EPStatement stmt = runtime.getDeploymentService().deploy(compiled).getStatements()[0];
        SupportUpdateListener listener = new SupportUpdateListener();
        stmt.addListener(listener);
        return listener;
    }
    @Test
    public void testInputHandling() throws EPCompileException, EPDeployException {
        String query = "select id, speed, timestamp from SensorData;";
        SupportUpdateListener listener = setStatement(query);
        long currentTime = System.currentTimeMillis();
        SensorData testEvent = new SensorData(1, Arrays.asList(20.0, 23.5, 18.0), currentTime);
        runtime.getEventService().sendEventBean(testEvent, "SensorData");
        EPAssertionUtil.assertProps(listener.assertOneGetNew(), "id,speed,timestamp".split(","), new Object[]{1, Arrays.asList(20.0, 23.5, 18.0), currentTime});
    }

    @Test
    public void testAvgByKey() throws EPDeployException, EPCompileException {
        String query = "select id, avg(speed) as averageSpeed from FlattenedData group by id;";
        SupportUpdateListener listener = setStatement(query);
        int id = 1;
        List<Double> speed = Arrays.asList(20.0, 23.5, 18.0);
        long timestamp = System.currentTimeMillis();
        List<FlattenedData> flattenedData = new ArrayList<>();
        speed.forEach(s -> flattenedData.add(new FlattenedData(id, s, timestamp)));
        flattenedData.forEach(data -> runtime.getEventService().sendEventBean(data, "FlattenedData"));
        EPAssertionUtil.assertProps(listener.getLastNewData()[0], "id,averageSpeed".split(","), new Object[]{1, 20.5});
    }
}