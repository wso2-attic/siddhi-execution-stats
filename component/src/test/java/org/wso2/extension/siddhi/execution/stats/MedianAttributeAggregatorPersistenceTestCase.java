/*
* Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.wso2.extension.siddhi.execution.stats;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class MedianAttributeAggregatorPersistenceTestCase {
    private static final Logger log = Logger.getLogger(MedianAttributeAggregatorPersistenceTestCase.class);
    private int count = 0;

    @Test
    public void testPersistence1() throws InterruptedException {
        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition = "define stream inputStream (tt double); " +
                "define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select stats:median(tt) as tt insert into filteredOutputStream";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(8.94775, ev.getData()[0]);
                            break;
                        case 2:
                            AssertJUnit.assertEquals(8.81493, ev.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 4:
                            AssertJUnit.assertEquals(8.56327, ev.getData()[0]);
                            break;
                        case 5:
                            AssertJUnit.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 6:
                            AssertJUnit.assertEquals(8.68211, ev.getData()[0]);
                            break;
                        case 7:
                            AssertJUnit.assertEquals(9.76563, ev.getData()[0]);
                            break;
                        case 8:
                            AssertJUnit.assertEquals(9.76563, ev.getData()[0]);
                            break;
                        case 9:
                            AssertJUnit.assertEquals(9.76563, ev.getData()[0]);
                            break;
                        case 10:
                            AssertJUnit.assertEquals(9.17144, ev.getData()[0]);
                            break;
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{8.94775});
        inputHandler.send(new Object[]{8.68211});
        inputHandler.send(new Object[]{8.44443});
        inputHandler.send(new Object[]{8.23472});
        inputHandler.send(new Object[]{10.9959});
        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
        }
        inputHandler.send(new Object[]{10.3738});
        inputHandler.send(new Object[]{9.76563});
        inputHandler.send(new Object[]{9.17144});
        inputHandler.send(new Object[]{8.19278});
        inputHandler.send(new Object[]{7.49374});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testPersistence2() throws InterruptedException {
        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition = "define stream inputStream (tt int); " +
                "define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select stats:median(tt) as tt insert into filteredOutputStream";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(1.0, ev.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(1.5, ev.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(2.0, ev.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(2.5, ev.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(3.0, ev.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(4.0, ev.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(5.0, ev.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals(6.0, ev.getData(0));
                            break;
                        case 9:
                            AssertJUnit.assertEquals(8.0, ev.getData(0));
                            break;
                        case 10:
                            AssertJUnit.assertEquals(8.0, ev.getData(0));
                            break;
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{1});
        inputHandler.send(new Object[]{2});
        inputHandler.send(new Object[]{3});
        inputHandler.send(new Object[]{4});
        inputHandler.send(new Object[]{5});
        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
        }
        inputHandler.send(new Object[]{6});
        inputHandler.send(new Object[]{8});
        inputHandler.send(new Object[]{9});
        inputHandler.send(new Object[]{10});
        inputHandler.send(new Object[]{7});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testPersistence3() throws InterruptedException {
        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition = "define stream inputStream (tt float); " +
                "define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select stats:median(tt) as tt insert into filteredOutputStream";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(1.5, ev.getData()[0]);
                            break;
                        case 2:
                            AssertJUnit.assertEquals(2.0, ev.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(2.5, ev.getData()[0]);
                            break;
                        case 4:
                            AssertJUnit.assertEquals(3.0, ev.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(3.5, ev.getData()[0]);
                            break;
                        case 6:
                            AssertJUnit.assertEquals(4.5, ev.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(5.5, ev.getData()[0]);
                            break;
                        case 8:
                            AssertJUnit.assertEquals(6.5, ev.getData(0));
                            break;
                        case 9:
                            AssertJUnit.assertEquals(8.5, ev.getData()[0]);
                            break;
                        case 10:
                            AssertJUnit.assertEquals(8.5, ev.getData(0));
                            break;
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{1.5f});
        inputHandler.send(new Object[]{2.5f});
        inputHandler.send(new Object[]{3.5f});
        inputHandler.send(new Object[]{4.5f});
        inputHandler.send(new Object[]{5.5f});
        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
        }
        inputHandler.send(new Object[]{6.5f});
        inputHandler.send(new Object[]{10.5f});
        inputHandler.send(new Object[]{9.5f});
        inputHandler.send(new Object[]{8.5f});
        inputHandler.send(new Object[]{7.5f});
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testPersistence4() throws InterruptedException {
        log.info("MedianAggregatorTestCase Double Sliding Length Window TestCase");
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String inStreamDefinition = "define stream inputStream (tt long); " +
                "define stream outputStream (tt double);";

        String query = "@info(name = 'query1') " + "from inputStream#window.length(5) " +
                "select stats:median(tt) as tt insert into filteredOutputStream";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("filteredOutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event ev : events) {
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(1.0, ev.getData()[0]);
                            break;
                        case 2:
                            AssertJUnit.assertEquals(1.5, ev.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(2.0, ev.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(2.5, ev.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(3.0, ev.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(4.0, ev.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(5.0, ev.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals(6.0, ev.getData(0));
                            break;
                        case 9:
                            AssertJUnit.assertEquals(8.0, ev.getData(0));
                            break;
                        case 10:
                            AssertJUnit.assertEquals(8.0, ev.getData(0));
                            break;
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{1L});
        inputHandler.send(new Object[]{2L});
        inputHandler.send(new Object[]{3L});
        inputHandler.send(new Object[]{4L});
        inputHandler.send(new Object[]{5L});
        siddhiAppRuntime.persist();
        siddhiAppRuntime.shutdown();
        inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();
        try {
            siddhiAppRuntime.restoreLastRevision();
        } catch (CannotRestoreSiddhiAppStateException e) {
            log.error(e.getMessage(), e);
        }
        inputHandler.send(new Object[]{10L});
        inputHandler.send(new Object[]{9L});
        inputHandler.send(new Object[]{6L});
        inputHandler.send(new Object[]{8L});
        inputHandler.send(new Object[]{7L});
        siddhiAppRuntime.shutdown();
    }
}
