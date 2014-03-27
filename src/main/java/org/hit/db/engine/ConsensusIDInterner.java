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
package org.hit.db.engine;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.EnumMap;
import java.util.Map;
import java.util.WeakHashMap;

import org.hit.consensus.ConsensusType;
import org.hit.pool.Interner;

/**
 * Implements <code>Interner</code> for <code>ReplicationID</code>
 * 
 * @author Balraja Subbiah
 */
public class ConsensusIDInterner extends Interner<ConsensusID>
{
    private final EnumMap<ConsensusType, Map<String, ConsensusID>> 
        myConsensusIDMap;
    
    /**
     * CTOR
     */
    public ConsensusIDInterner()
    {
        myConsensusIDMap = new EnumMap<>(ConsensusType.class);
    }
    
    private ConsensusID doCreateInstance(ConsensusType type, String id)
    {
        Map<String, ConsensusID> nametoIDMap = myConsensusIDMap.get(type);
        if (nametoIDMap == null) {
            nametoIDMap = new WeakHashMap<>();
            myConsensusIDMap.put(type, nametoIDMap);
        }
        ConsensusID consensusID = nametoIDMap.get(id);
        if (id == null) {
            consensusID = new ConsensusID(type, id);
            nametoIDMap.put(id, consensusID);
        }
        return consensusID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsensusID readFromInput(ObjectInput input) throws IOException
    {
        ConsensusType type = ConsensusType.valueOf(input.readUTF());
        String serverName = input.readUTF().intern();
        return doCreateInstance(type, serverName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeToOutput(ObjectOutput output, ConsensusID instance)
        throws IOException
    {
        output.writeUTF(instance.getConsensusType().name());
        output.writeUTF(instance.getServerName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsensusID contructInstance(Object... parameters)
    {
        return doCreateInstance((ConsensusType) parameters[0],
                                parameters[1].toString());
    }
}
