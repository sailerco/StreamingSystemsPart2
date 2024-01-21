package org.example;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

class ListenerTest {
    EPRuntime runtime;
    @BeforeEach
    void setUp() {
        runtime = mock(EPRuntime.class);
    }

    @Test
    public void testSendEvent(){
        Listener.SensorEventListener sensorEventListener = new Listener.SensorEventListener();
        SensorData sensorData = new SensorData(1, List.of(-15.0, 20.0, 25.0), System.currentTimeMillis());
        EventBean eventBean = mock(EventBean.class);
        when(eventBean.get("id")).thenReturn(1);
        when(eventBean.get("speed")).thenReturn(new ArrayList<>(List.of(-15.0, 20.0, 25.0)));
        when(eventBean.get("timestamp")).thenReturn(System.currentTimeMillis());
        // when
        sensorEventListener.update(new EventBean[]{eventBean}, null, null, runtime);

        // then
        verify(runtime, times(1)).getEventService().sendEventBean(any(FlattenedData.class), eq("FlattenedData"));
    }
}