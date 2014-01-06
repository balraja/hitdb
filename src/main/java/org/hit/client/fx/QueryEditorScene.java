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

import java.net.URISyntaxException;

import org.hit.client.Constants;

import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.geometry.VPos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.SeparatorBuilder;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextAreaBuilder;
import javafx.scene.image.Image;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;

/**
 * A class to abstract out the various components of a scene graph 
 * displayed in the visual shell.
 * 
 * @author Balraja Subbiah
 */
public class QueryEditorScene
{
    private static final String WINDOW_TITLE = "Visual Shell - 0.1";
    
    private final BorderPane myScene;
    
    private final TextArea myQueryEditorArea;
    
    private final TabPane myQueryResulPanel;

    /**
     * CTOR
     */
    public QueryEditorScene()
    {
        myScene = new BorderPane();
        
        // Set the header 
        BorderPane header = new BorderPane();
        header.setPadding(new Insets(15, 12, 15, 12));
        header.setId("qp-header");
        Label banner = 
           new Label(Constants.TITLE + " - " + WINDOW_TITLE);
        banner.setAlignment(Pos.CENTER);
        banner.setId("qp-header-label");
        header.setCenter(banner);
        myScene.setTop(header);
        
        // Query pane
        GridPane layout = new GridPane();
        myQueryEditorArea = TextAreaBuilder.create()
                                       .prefRowCount(20)
                                       .prefColumnCount(500)
                                       .text(Constants.HELP_MSG)
                                       .style(" -fx-font: 12px calibiri;")
                                       .build();
        layout.addRow(0, myQueryEditorArea);
        GridPane.setHalignment(myQueryEditorArea, HPos.CENTER);
        GridPane.setValignment(myQueryEditorArea, VPos.CENTER);
        
        layout.addRow(1, SeparatorBuilder.create()
                                         .orientation(Orientation.HORIZONTAL)
                                         .build());
        
        // Result component
        myQueryResulPanel = new TabPane();
        Tab version = new Tab("About");
        StackPane aboutContent = new StackPane();
        Label aboutLabel = new Label(Constants.ABOUT);
        aboutLabel.setId("qp-about-text");
        Rectangle rectangle = new Rectangle(100, 100, Color.WHITE);
        rectangle.heightProperty().bind(aboutLabel.heightProperty().add(100));
        aboutContent.getChildren().addAll(rectangle, aboutLabel);
        version.setContent(aboutContent);
        myQueryResulPanel.getTabs().add(version);
        
        layout.addRow(2, myQueryResulPanel);
        GridPane.setHalignment(myQueryResulPanel, HPos.CENTER);
        GridPane.setValignment(myQueryResulPanel, VPos.CENTER);
        myScene.setCenter(layout);
        
    }
    
    /** A helper method to add the scene graph to the display stage */
    public void addTo(Stage stage)
    {
        Scene scene = new Scene(myScene);
        String stylePath;
        try {
            stylePath = getClass().getClassLoader()
                                  .getResource("query-editor.css")
                                  .toURI()
                                  .toString();
            
            scene.getStylesheets().add(stylePath);
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
        }
        
        stage.setScene(scene);
        stage.getIcons().add(
            new Image(getClass().getClassLoader()
                    .getResourceAsStream(Constants.APP_ICON_FILE)));
    }
}
