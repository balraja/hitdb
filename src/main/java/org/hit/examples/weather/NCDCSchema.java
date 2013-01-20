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

package org.hit.examples.weather;

import org.hit.db.apt.MetaColumns;
import org.hit.db.apt.MetaColumn;
import org.hit.db.apt.MetaTable;
import org.hit.db.model.PartitioningType;

import java.util.Date;

/**
 * AN interface that defines the schema of the table.
 * 
 * @author Balraja Subbiah
 */
@MetaTable(keyClass = Integer.class , tableName="NCDCData", partitioningType = PartitioningType.HASHABLE)
public interface NCDCSchema
{
    @MetaColumns( columns = {
        @MetaColumn(index = 0, isPrimary = true, name = "StationNumber", type = Integer.class),
        @MetaColumn(index = 1, isPrimary = false, name = "Date", type = Date.class),
        @MetaColumn(index = 1, isPrimary = false, name = "MeanTemperature", type = Integer.class)
    })
    public void getColumns();
}
