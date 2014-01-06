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

package org.hit.client.fx;

import javafx.application.Application;
import javafx.stage.Stage;

/**
 * An javafx application to provide a visual client for interacting with
 * hit database servers.
 * 
 * @author Balraja Subbiah
 */
public class VShell extends Application 
{
    /**
     * {@inheritDoc}
     */
	@Override
	public void start(Stage primaryStage) 
	{
	    QueryEditorScene queryEditor = new QueryEditorScene();
	    queryEditor.addTo(primaryStage);
	    primaryStage.show();
	}

	/**
	 * The main class that launches the application.
	 */
	public static void main(String[] args) 
	{
		launch(args);
	}
}
