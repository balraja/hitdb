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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.hit.db.keyspace.Keyspace;

/**
 * Defines the contract for a table's schema.
 *
 * @author Balraja Subbiah
 */
public class HitTableSchema extends TableSchema
{
    private Keyspace<?,?> myKeyspace;
    
    private Class<? extends Comparable<?>> myKeyClass;

    private Class<? extends Persistable<?>> myPersistableClass;

    private boolean myReplicated;

    /**
     * CTOR
     */
    public HitTableSchema()
    {
        this(null, null, null, null, null, null);
    }
    
    /**
     * CTOR
     */
    public HitTableSchema(
        String tableName,
        List<String> tableColumns,
        List<String> primaryColumns,
        Class<? extends Persistable<?>> persistableclass,
        Class<? extends Comparable<?>> keyclass,
        Keyspace<?,?> keyspace)
    {
        this(tableName, 
             tableColumns, 
             primaryColumns, 
             persistableclass, 
             keyclass, 
             keyspace, 
             false);
    }

    /**
     * CTOR
     */
    public HitTableSchema(
          String tableName,
          List<String> tableColumns,
          List<String> primaryColumns,
          Class<? extends Persistable<?>> persistableclass,
          Class<? extends Comparable<?>> keyclass,
          Keyspace<?,?> keyspace,
          boolean replicated)
    {
        super(tableName, tableColumns, primaryColumns);
        myPersistableClass = persistableclass;
        myKeyClass = keyclass;
        myKeyspace = keyspace;
        myReplicated = false;
    }

    /**
     * Returns the {@link Keyspace} that defines the possible values
     * to which the keys in a table can map to.
     */
    public Keyspace<?,?> getKeyspace()
    {
        return myKeyspace;
    }

    /**
     * Returns the value of keyType
     */
    public Class<? extends Comparable<?>> getKeyClass()
    {
        return myKeyClass;
    }

    /**
     * Returns the value of persistableClass
     */
    public Class<? extends Persistable<?>> getPersistableClass()
    {
        return myPersistableClass;
    }
    
    /**
     * Returns true if the table is to be replicated across all the nodes.
     */
    public boolean isReplicated()
    {
        return myReplicated;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myPersistableClass = (Class<? extends Persistable<?>>) in.readObject();
        myKeyClass = (Class<? extends Comparable<?>>) in.readObject();
        myKeyspace = (Keyspace<?,?>) in.readObject();
        myReplicated = in.readBoolean();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Schema [myTableName=" + getTableName()
               + ", myPersistableClass="
               + myPersistableClass
               + ", myKeyClass="
               + myKeyClass
               + ", myKeySpace="
               + myKeyspace
               + "isReplicated"
               + myReplicated
               + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myPersistableClass);
        out.writeObject(myKeyClass);
        out.writeObject(myKeyspace);
        out.writeBoolean(myReplicated);
    }
}
