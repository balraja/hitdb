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

package org.hit.db.apt;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.JavaFileObject;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

/**
 * Extends the <code>AbstractProcessor</code> for processing the
 * annotations.
 * 
 * @author Balraja Subbiah
 */
@SupportedAnnotationTypes(value={"org.hit.db.apt.MetaColumn",
                                 "org.hit.db.apt.MetaColumns",
                                 "org.hit.db.apt.MetaTable"})
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class HitAnnotationProcessor extends AbstractProcessor
{
    public static class FormattedMetaColumn
    {
        private final MetaColumn myMetaColumn ;
        
        private final int myIndex;

        /**
         * CTOR
         */
        public FormattedMetaColumn(MetaColumn metaColumn, int index)
        {
            super();
            myMetaColumn = metaColumn;
            myIndex = index;
        }
        
        public String getName()
        {
            return myMetaColumn.name();
        }
        
        public String getVariableName()
        {
            return Character.toLowerCase(myMetaColumn.name().charAt(0))
                   + myMetaColumn.name().substring(1);
        }
        
        public boolean isPrimary()
        {
            return myMetaColumn.primary();
        }
        
        public int getIndex()
        {
            return myIndex;
        }
        
        public String getType()
        {
            int lastIndex = myMetaColumn.type().lastIndexOf('.');
            return  lastIndex > -1 ? myMetaColumn.type().substring(lastIndex + 1)
                                   : myMetaColumn.type();
        }
        
        public boolean isImportNecessary()
        {
            return myMetaColumn.type().indexOf('.') > -1;
        }
        
        public String getQualifiedType()
        {
            return myMetaColumn.type();
        }
    }
    
    /**
     * CTOR
     */
    public HitAnnotationProcessor()
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv)
    {
        super.init(processingEnv);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations,
                           RoundEnvironment           roundEnv)
    {
        for (Element e : roundEnv.getElementsAnnotatedWith(MetaTable.class)) {
            
            if (e.getKind() == ElementKind.INTERFACE) {
                
                TypeElement typeElement = (TypeElement) e;
                
                PackageElement packageElement = 
                    (PackageElement) typeElement.getEnclosingElement();

                MetaTable metaTable =
                    typeElement.getAnnotation(MetaTable.class);
                
                List<MetaColumn> metaColumns = new ArrayList<>();
                
                for (Element enclosedElement :
                        typeElement.getEnclosedElements())
                {
                    if (enclosedElement.getKind() == ElementKind.METHOD) {
                        MetaColumns metaColumnsAnnotation =
                            enclosedElement.getAnnotation(MetaColumns.class);
                        
                        if (metaColumnsAnnotation != null) {
                            metaColumns.addAll(
                                Arrays.asList(metaColumnsAnnotation.columns()));
                        }
                    }
                }
                
                try {
                    JavaFileObject jfo =
                        processingEnv.getFiler().createSourceFile(
                            metaTable.tableName());

                    Writer writer = jfo.openWriter();

                    STGroup group = new STGroupFile("db_model_templates.stg");
                    ST st = group.getInstanceOf("dbmodel");
                    st.add("packageName", 
                           packageElement.getQualifiedName().toString());
                    st.add("tableName", metaTable.tableName());
                    st.add("keyClassName", metaTable.keyClassName());
                   
                    List<FormattedMetaColumn> formattedColumns = 
                        new ArrayList<>();
                        int index = 0;
                    for (MetaColumn column : metaColumns) {
                        formattedColumns.add(new FormattedMetaColumn(column, 
                                                                     index));
                        index++;
                    }
                    st.add("metaColumns", formattedColumns);

                    writer.write(st.render());
                    writer.flush();
                    writer.close();
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return true;
    }
}
