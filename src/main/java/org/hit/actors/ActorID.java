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

package org.hit.actors;

import org.hit.concurrent.epq.AccessorID;

/**
 * The identifier for an actor. We abstract this as a type because it will
 * give us flexibility to change the identification mechanism if required later.
 *
 * @author Balraja Subbiah
 */
public enum ActorID implements AccessorID
{
    COMMUNICATOR,
    DB_ENGINE,
    GROUP_MANAGER,
    SERVER_COMPONENT_MANAGER,
    CONSENSUS_MANAGER,
    GOSSIPER
}
