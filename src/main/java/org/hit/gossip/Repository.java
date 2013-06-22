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

package org.hit.gossip;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The repository that holds all the <code>Information</code> shared among 
 * the nodes.
 * 
 * @author Balraja Subbiah
 */
public class Repository implements Cloneable
{
    private final Map<Serializable, Gossip> myKeyToInformationMap;
    
    /**
     * CTOR
     */
    public Repository()
    {
        myKeyToInformationMap = new HashMap<>();
    }
    
    /**
     * CTOR
     */
    public Repository(
        Map<Serializable, Gossip> keyToInformationMap)
    {
        super();
        myKeyToInformationMap = keyToInformationMap;
    }

    /**
     * Returns <code>Information</code> corresponding to the key.
     */
    public Gossip lookup(Serializable key)
    {
        return myKeyToInformationMap.get(key);
    }
    
    /**
     * Updates the key and value pairs.
     */
    public void update(Gossip gossip)
    {
        myKeyToInformationMap.put(gossip.getKey(), gossip);
    }
    
    /**
     * Updates the key and value pairs.
     */
    public void update(List<Gossip> updates)
    {
        for (Gossip i : updates) {
            myKeyToInformationMap.put(i.getKey(), i);
        }
    }
    
    /** 
     * Returns the <code>Digest</code> for information stored in the 
     * repository.
     */
    public Digest makeDigest()
    {
        TObjectLongMap<Serializable> keyToVersionMap = 
            new TObjectLongHashMap<>();
            
        for (Map.Entry<Serializable, Gossip> entry : 
                myKeyToInformationMap.entrySet())
        {
            keyToVersionMap.put(entry.getKey(), 
                                entry.getValue().getTimestamp());
        }
        
        return new Digest(keyToVersionMap);
    }
    
    /**
     * A helper method to process the digest and return latest <code>
     * Information</code> whose version is greater than the version
     * number specified in the digest.
     */
    public List<Gossip> processDigest(Digest digest)
    {
        List<Gossip> latestInformation = new ArrayList<>();
        for (Map.Entry<Serializable, Gossip> entry : 
                myKeyToInformationMap.entrySet())
        {
            long otherVersion = digest.getKeyToVersionMap().get(entry.getKey());
            if (entry.getValue().getTimestamp() > otherVersion) {
                latestInformation.add(entry.getValue());
            }
            else if (otherVersion 
                            == digest.getKeyToVersionMap().getNoEntryValue())
            {
                latestInformation.add(entry.getValue());
            }
        }
        return latestInformation;
    }
    
    /**
     * CTOR
     */
    public Repository makeCopy()
    {
        return new Repository(myKeyToInformationMap);
    }
}
