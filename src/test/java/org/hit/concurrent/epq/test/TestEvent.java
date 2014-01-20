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
package org.hit.concurrent.epq.test;

import org.hit.event.Event;

/**
 * An ordered dummy {@link Event} to be used for testing.
 * 
 * @author Balraja Subbiah
 */
public class TestEvent implements Event,Comparable<TestEvent>
{
    private final int myEventID;

    /**
     * CTOR
     */
    public TestEvent(int eventID)
    {
        super();
        myEventID = eventID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TestEvent o)
    {
        return myEventID - o.myEventID;
    }

    /**
     * Returns the value of eventID
     */
    public int getEventID()
    {
        return myEventID;
    }
}
