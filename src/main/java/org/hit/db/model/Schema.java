/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.db.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.hit.distribution.KeySpace;

/**
 * Defines the contract for schema of a table.
 * 
 * @author Balraja Subbiah
 */
public class Schema implements Externalizable
{
    private String myTableName;

    private Class<? extends Persistable<?>> myPersistableClass;

    private Class<? extends Comparable<?>>  myKeyClass;

    private List<Column> myColumns;

    private PartitioningType myKeyPartitioningType;
    
    private final KeySpace<? extends Comparable<?>> myHashRing;

    /**
     * CTOR
     */
    public Schema(String tableName,
                  List<Column> columns,
                  Class<? extends Persistable<?>> persistableclass,
                  Class<? extends Comparable<?>> keyclass,
                  PartitioningType keyType,
                  KeySpace<? extends Comparable<?>> hashRing)
    {
        myColumns = columns;
        myTableName = tableName;
        myPersistableClass = persistableclass;
        myKeyClass = keyclass;
        myKeyPartitioningType = keyType;
        myHashRing = hashRing;
    }

    /**
     * Returns the value of columns
     */
    public List<Column> getColumns()
    {
        return myColumns;
    }
    
    /**
     * Returns the value of hashRing
     */
    public KeySpace<? extends Comparable<?>> getHashRing()
    {
        return myHashRing;
    }

    /**
     * Returns the value of keyType
     */
    public Class<? extends Comparable<?>> getKeyClass()
    {
        return myKeyClass;
    }

    /**
     * Returns the value of keyPartitoiningType
     */
    public PartitioningType getKeyPartitioningType()
    {
        return myKeyPartitioningType;
    }

    /**
     * Returns the value of persistableClass
     */
    public Class<? extends Persistable<?>> getPersistableClass()
    {
        return myPersistableClass;
    }

    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myTableName = in.readUTF();
        myPersistableClass = (Class<? extends Persistable<?>>) in.readObject();
        myKeyClass = (Class<? extends Comparable<?>>) in.readObject();
        myColumns = (List<Column>) in.readObject();
        myKeyPartitioningType = (PartitioningType) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myTableName);
        out.writeObject(myPersistableClass);
        out.writeObject(myKeyClass);
        out.writeObject(myColumns);
        out.writeObject(myKeyPartitioningType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Schema [myTableName=" + myTableName
               + ", myPersistableClass="
               + myPersistableClass
               + ", myKeyClass="
               + myKeyClass
               + ", myColumns="
               + myColumns
               + ", myKeyPartitioningType="
               + myKeyPartitioningType
               + ", myHashRing="
               + myHashRing
               + "]";
    }
}
