/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease. 

    Copyright (C) 2013  Balraja Subbiah

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.hit.transactions.test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hit.db.engine.TransactionManager;
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.event.Event;
import org.hit.event.SendMessageEvent;
import org.hit.messages.DBOperationSuccessMessage;
import org.hit.time.SimpleSystemClock;
import org.junit.Test;
import org.junit.Assert;

/**
 * Defines a test case for verifying a simple workflow in the 
 * {@link TransactionManager}.
 * 
 * @author Balraja Subbiah
 */
public class SimpleWorkflowTest
{
    private AtomicBoolean myIsDone = new AtomicBoolean(false);
    
    private volatile boolean myIsUpdatePhase = true;
    
    private TransactionManager myTransactionManager;
    
    @Test
    public void updateAndQueryTest()
    {
        RecordingEventBus recordingEventBus = 
            new RecordingEventBus(
                Collections.<Class<? extends Event>>singleton(
                    SendMessageEvent.class),
                 new EventNotificationListener() {
                    @Override
                    public void handleEvent(Event recordedEvent)
                    {
                        Assert.assertTrue(
                            recordedEvent instanceof SendMessageEvent);
                        
                        SendMessageEvent sendMessageEvent = 
                           (SendMessageEvent) recordedEvent;
                        Assert.assertTrue(sendMessageEvent.getTargets().contains(
                            TestID.CLIENT_NODE_ID));
                        Assert.assertTrue(
                            sendMessageEvent.getMessage() 
                                instanceof DBOperationSuccessMessage);
                        DBOperationSuccessMessage success = 
                            (DBOperationSuccessMessage) 
                                sendMessageEvent.getMessage();
                        
                        if (myIsUpdatePhase) {
                            Assert.assertEquals(1L, success.getSequenceNumber());
                            myIsUpdatePhase = false;
                            myTransactionManager.processOperation(
                                TestID.CLIENT_NODE_ID, 
                                new BalanceQuery(1L), 
                                2L);
                        }
                        else {
                            Assert.assertEquals(2L, success.getSequenceNumber());
                            Assert.assertEquals(100.0D, success.getResult());
                            myIsDone.compareAndSet(false, true);
                        }
                     }
                });
        
        myTransactionManager =
            new TransactionManager(
                new TransactableHitDatabase(), 
                new SimpleSystemClock(),
                recordingEventBus,
                TestID.SERVER_NODE_ID, 
                null);
        myTransactionManager.createTable(Account.SCHEMA);
        myTransactionManager.processOperation(TestID.CLIENT_NODE_ID, 
                                 new UpdateBalanceTransaction(1L, 100.0D), 
                                 1L);
        
        while (!myIsDone.get()) {
            // Busy wait.
        }
        
    }
}
