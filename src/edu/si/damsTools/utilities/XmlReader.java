/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import java.util.ArrayList;


/**
 *
 * @author rfeldman
 */
public class XmlReader {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
            
    public ArrayList parser(String mainXmlBlock, String tag) {
      
        String xmlFilename = DamsTools.getProperty(DamsTools.getOperationType() + "XmlFile");
        
        logger.log(Level.FINEST, "Reading xmlFile: " + DamsTools.getDirectoryName() + "/" + xmlFilename);
        String inputfile = DamsTools.getDirectoryName() + "/" + xmlFilename;
        
        ArrayList xmlObjList = new ArrayList();
                
        try {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);

        XMLEventReader eventReader =
        factory.createXMLEventReader(new FileReader(inputfile));
         
        boolean queryInd = false;
        boolean operationInd = false;
        boolean finished = false;
        
        XmlQueryData xmlTagData = null;
        
        while(eventReader.hasNext() && !finished) {
            XMLEvent event = eventReader.nextEvent();
               
            switch(event.getEventType()) {
               
                case XMLStreamConstants.START_ELEMENT:
                    StartElement startElement = event.asStartElement();
                    String qName = startElement.getName().getLocalPart();

                    if (qName.equals(mainXmlBlock)) {
                        operationInd = true;
                    }
                    
                    if (operationInd && qName.equals(tag)) {
                        xmlTagData = new XmlQueryData();
                        
                        Iterator<Attribute> attributes = event.asStartElement().getAttributes();
                        while(attributes.hasNext()){

                            Attribute attribute = attributes.next();
                            logger.log(Level.FINEST, "Attribute info: " + attribute.getName().toString() + "VALUE " + attribute.getValue());
                            
                            xmlTagData.addAttribute(attribute.getName().toString(), attribute.getValue());
                         }
                        //System.out.println("Type : " + type);
                        queryInd = true;
                    }
                              
                break;

                case XMLStreamConstants.CHARACTERS:
                    Characters characters = event.asCharacters();
                    if(queryInd) {          
                        xmlTagData.setDataValue(characters.getData());
                        logger.log(Level.FINEST, "found information from xml parse: " + characters.getData());
                        xmlObjList.add(xmlTagData);    
                        queryInd = false;
                    }
                break;

                case XMLStreamConstants.END_ELEMENT:
                    EndElement endElement = event.asEndElement();
                    
                    if (endElement.getName().getLocalPart().equals(mainXmlBlock)) {
                        finished = true;
                    }
                break;
            } 
        }
        } catch (FileNotFoundException e) {
         e.printStackTrace();
        } catch (XMLStreamException e) {
         e.printStackTrace();
        }
        
        return xmlObjList;
    }
    
    

}
