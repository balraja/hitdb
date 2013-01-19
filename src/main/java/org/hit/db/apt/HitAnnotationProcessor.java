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
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.JavaFileObject;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

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
    private Template myClassTemplate;
    
    private ProcessingEnvironment myProcessingEnvironment = null;
    
    private TypeMirror myMetaColumnsType = null;
    
    /**
     * CTOR
     */
    public HitAnnotationProcessor()
    {
        
        try {
            Properties props = new Properties();
            props.put("runtime.log.logsystem.class",
                      "org.apache.velocity.runtime.log.SystemLogChute");
            props.put("resource.loader", "classpath");
            props.put("classpath.resource.loader.class",
                      "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            VelocityEngine ve = new VelocityEngine(props);
            ve.init();
            myClassTemplate = ve.getTemplate("beaninfo.vm");
        }
        catch (ResourceNotFoundException | ParseErrorException e)
        {
            myClassTemplate = null;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv)
    {
        super.init(processingEnv);
        myProcessingEnvironment = processingEnv;
        Element metaColumnsElement =
            myProcessingEnvironment.getElementUtils()
                                   .getTypeElement(MetaColumns.class.getName());
        myMetaColumnsType = metaColumnsElement.asType();
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
                
                typeElement.getEnclosingElement();

                MetaTable metaTable =
                    typeElement.getAnnotation(MetaTable.class);
                
                List<MetaColumn> columns = new ArrayList<>();
                
                for (Element enclosedElement :
                        typeElement.getEnclosedElements())
                {
                    if (enclosedElement.getKind() == ElementKind.METHOD) {
                        for (AnnotationMirror annotation :
                                enclosedElement.getAnnotationMirrors())
                        {
                            if (annotation.getAnnotationType().equals(
                                    myMetaColumnsType))
                            {
                                MetaColumns metaColumns =
                                   annotation.getAnnotationType()
                                             .asElement()
                                             .getAnnotation(MetaColumns.class);
                                
                                if (metaColumns != null) {
                                    columns.addAll(
                                        Arrays.asList(metaColumns.columns()));
                                }
                            }
                        }
                    }
                }
                
                try {
                    JavaFileObject jfo =
                        processingEnv.getFiler().createSourceFile(
                            metaTable.tableName());

                    Writer writer = jfo.openWriter();

                    VelocityContext vc = new VelocityContext();
                    vc.put("tableName", metaTable.tableName());
                    vc.put("keyClassName", metaTable.keyClass().getSimpleName());
                    vc.put("meta_columns", columns);
                    myClassTemplate.merge(vc, writer);

                    writer.close();
                }
                catch (ResourceNotFoundException | ParseErrorException
                       | MethodInvocationException | IOException e1)
                {
                    e1.printStackTrace();
                }
            }
        }
        return true;
    }
}
