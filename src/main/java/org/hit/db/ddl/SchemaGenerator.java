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

package org.hit.db.ddl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Defines the type that can be used for parsing the schema file represented
 * as xml. 
 * 
 * @author Balraja Subbiah
 */
public class SchemaGenerator
{                                                     
    private static final String TABLE_ELEMENT = "table";
    
    private static final String COLUMN_ELEMENT = "column";
    
    private static final String NAME = "name";
    
    private static final String TYPE = "type";
    
    private static final String KEY_TYPE = "keyClass";
    
    private static final String IS_PRIMARY = "isPrimary";
    
    private static final String DOT_SEPARATOR = "\\.";
    
    private static final String JAVA_EXTN = ".java";
    
    private static final String SCHEMA_FILE_NAME = "schema.xml";
    
    private static final String TEMPLATE_FILE_NAME = "db_model_templates.stg";
    
    private static final String TEMPLATE_NAME = "dbmodel";
    
    private static final String PACKAGE_NAME_ARG = "packageName";
    
    private static final String TABLE_NAME_ARG = "tableName";
    
    private static final String KEY_CLASS_NAME_ARG = "keyClassName";
    
    private static final String META_COLUMNS_ARG = "metaColumns";
       
    /**
     * Defines the contract for a context to be used when parsing the 
     * xml file.
     */
    private static class Context
    {
        private List<MetaColumn> myColumnInfo;
        
        private MetaTable myTable;

        /**
         * CTOR
         */
        public Context()
        {
            myColumnInfo = new ArrayList<>();
        }

        /**
         * Returns the value of table
         */
        public MetaTable getTable()
        {
            return myTable;
        }

        /**
         * Setter for the table
         */
        public void setTable(MetaTable table)
        {
            myTable = table;
        }

        /**
         * Returns the value of columnInfo
         */
        public List<MetaColumn> getColumnInfo()
        {
            return myColumnInfo;
        }
    }
    
    private class TableElementHandler extends DefaultHandler
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void startElement(String uri,
                                 String localName,
                                 String qName,
                                 Attributes attributes) throws SAXException
        {
            String tableName = attributes.getValue(NAME);
            String keyType   = attributes.getValue(KEY_TYPE);
            getContext().setTable(new MetaTable(keyType, tableName));
        }
    }
    
    private class ColumnElementHandler extends DefaultHandler
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void startElement(
                                 String uri,
                                 String localName,
                                 String qName,
                                 Attributes attributes) throws SAXException
        {
            String columnName = attributes.getValue(NAME);
            String columnType = attributes.getValue(TYPE);
            boolean isPrimary  = 
                Boolean.valueOf(attributes.getValue(IS_PRIMARY));
            
            int index = getContext().getColumnInfo().size();
            getContext().getColumnInfo()
                        .add(new MetaColumn(isPrimary, 
                                            index, 
                                            columnName,
                                            columnType));
        }
    }
    
    private class ElementHandler extends DefaultHandler
    {
        private final Map<String, DefaultHandler> myElementToHandler;

        /**
         * CTOR
         */
        public ElementHandler()
        {
            super();
            myElementToHandler = new HashMap<String, DefaultHandler>();
            myElementToHandler.put(TABLE_ELEMENT, new TableElementHandler());
            myElementToHandler.put(COLUMN_ELEMENT, new ColumnElementHandler());
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void startElement(
                                 String uri,
                                 String localName,
                                 String qName,
                                 Attributes attributes) throws SAXException
        {
            super.startElement(uri, localName, qName, attributes);
            DefaultHandler handler = myElementToHandler.get(qName);
            if (handler != null) {
                handler.startElement(uri, localName, qName, attributes);
            }
        }
    }
    
    /**
     * Extends {@link SimpleFileVisitor} to support visiting all files 
     * under a given directory.
     */
    private class SchemaFileVisitor extends SimpleFileVisitor<Path>
    {
        private final Path mySourceDirectory;
        
        private Path myCurrentDirectory;
        
        /**
         * CTOR
         */
        public SchemaFileVisitor(Path sourceDirectory)
        {
            super();
            mySourceDirectory = sourceDirectory;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult preVisitDirectory(Path dir,
                BasicFileAttributes attrs) throws IOException
        {
            myCurrentDirectory = dir;
            return super.preVisitDirectory(dir, attrs);
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException
        {
            if (file.getFileName().endsWith(SCHEMA_FILE_NAME)) {
                parseAndGenerateTable(
                    file.toFile(), myCurrentDirectory, mySourceDirectory);
            }
            return super.visitFile(file, attrs);
        }
        
    }
    
    private final Context myContext;
    
    /**
     * CTOR
     */
    public SchemaGenerator()
    {
        myContext = new Context();
    }

    /**
     * Returns the value of context
     */
    public Context getContext()
    {
        return myContext;
    }
    
    private String inferPackageName(Path srcDirectory, Path packageDirectory)
    {
        Path packagePath = srcDirectory.relativize(packageDirectory);
        return packagePath.toString().replace(File.separatorChar, '.');
    }
    
    private String makeJavaFileName(String tableName)
    {
        String fileName = 
            Character.toUpperCase(tableName.charAt(0))
            + tableName.substring(1).toLowerCase()
            + JAVA_EXTN;
                        
        return fileName;
    }
    
    /**
     * A helper method to parse schema information out of an xml file 
     * and generate classes out of it.
     */
    private void parseAndGenerateTable(File file, 
                                       Path packageDirectory,
                                       Path srcDirectory)
    {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.newSAXParser().parse(file, new ElementHandler());
            
            File tableType = new File(packageDirectory.toFile(),
                                      makeJavaFileName(
                                          myContext.getTable().getTableName()));
            if (tableType.exists()) {
                tableType.delete();
            }
            Writer writer = new FileWriter(tableType);

            STGroup group = new STGroupFile(TEMPLATE_FILE_NAME);
            ST st = group.getInstanceOf(TEMPLATE_NAME);
            st.add(PACKAGE_NAME_ARG, 
                    inferPackageName(srcDirectory, packageDirectory));
            
            st.add(TABLE_NAME_ARG, getContext().getTable().getTableName());
            st.add(KEY_CLASS_NAME_ARG, getContext().getTable().getKeyTypeName());

            st.add(META_COLUMNS_ARG, getContext().getColumnInfo());

            writer.write(st.render());
            writer.flush();
            writer.close();
        }
        catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Starts from the specified directory and recursively visits all 
     * subdirectories. 
     */
    public void generateTableSourceFiles(Path startDirectory)
    {
        try {
            Files.walkFileTree(startDirectory, 
                               new SchemaFileVisitor(startDirectory));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
