/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.io.FileReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
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
    
    private XMLEventReader eventReader;
    private XMLInputFactory factory;
    private final String mainOperationXmlBlock;
    private final String subXmlBlock;
    private final String xmlFile;
    private final ArrayList <XmlData> xmlObjList;
    
    // This constructor has subOperation
    public XmlReader (String xmlFile, String mainXmlBlock, String subXmlBlock) {
        this.xmlFile = xmlFile;
        this.mainOperationXmlBlock = mainXmlBlock;
        this.subXmlBlock = subXmlBlock;
        xmlObjList = new ArrayList();
    }
            
    public ArrayList returnXmlObjList () {
        return this.xmlObjList;
    }
    
    public ArrayList <XmlData> parseReturnXmlObjectList() {

        logger.log(Level.FINEST, "XML file is at: " + xmlFile );
        
        try {
            factory = XMLInputFactory.newInstance();
            factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);

            eventReader = factory.createXMLEventReader(new FileReader(xmlFile));
         
            boolean insideOpBlockInd = false;
            boolean insideSubBlock = false;
            boolean finished = false;
            XmlData xmlTagData = null;
            
            while(eventReader.hasNext() && !finished) {
                XMLEvent event = eventReader.nextEvent();
               
                switch(event.getEventType()) {
               
                    case XMLStreamConstants.START_ELEMENT:

                        StartElement startElement = event.asStartElement();
                        String qName = startElement.getName().getLocalPart();

                        // If we are already identified as being inside the block we need,
                        //once we set this, we loop through this again and next iteration we get the tag info
                        if (qName.equals(mainOperationXmlBlock)) {
                            insideOpBlockInd = true;
                            //Loop through again to see if we can set the data
                            //logger.log(Level.FINEST,"DEBUG In Main operation");
                            continue;
                        }

                        if (insideOpBlockInd && !insideSubBlock) {
                             insideSubBlock = checkCorrectSubBlock(qName);
                             //Loop through again to get the data fro this subBlock
                             if (insideSubBlock) {
                                if (! mainOperationXmlBlock.equals("global") && subXmlBlock != null) {
                                    //logger.log(Level.FINEST,"DEBUG In Sub-operation");
                                    continue;
                                }
                             }
                        }
                        
                        //populate the name and attribures
                        if (insideSubBlock) {
                            logger.log(Level.FINEST,"DEBUG Setting tag: " + qName);
                            xmlTagData = new XmlData(qName);
                            Iterator<Attribute> attributes = event.asStartElement().getAttributes();
                            while(attributes.hasNext()){
                                Attribute attribute = attributes.next();
                                logger.log(Level.FINEST, "DEBUG: Attribute info: " + attribute.getName().toString() + " VALUE " + attribute.getValue());
                                xmlTagData.addAttribute(attribute.getName().toString(), attribute.getValue());
                            }                
                        }                                           
                    break;

                    case XMLStreamConstants.CHARACTERS:
                        Characters characters = event.asCharacters();
                        if (characters.isWhiteSpace()) {
                        //skip if this is blank space
                            continue;
                        }
 
                        if(xmlTagData != null) {                             
                            xmlTagData.setDataValue(characters.getData().trim());
                            xmlObjList.add(xmlTagData);                
                        }
                    break;

                    case XMLStreamConstants.END_ELEMENT:
                        // If we are finished, then mark as finished.  we can ignore the rest of the file
                        EndElement endElement = event.asEndElement();
                        finished = markFinished(endElement.getName().getLocalPart(), insideOpBlockInd, insideSubBlock );
                        
                    break;
                } 
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        
        return xmlObjList;
    }
    
    private boolean checkCorrectSubBlock (String tagName) {
        
        //Now that we are in the main block that we need...
        if (subXmlBlock == null ) {
            //subXmlBlock Does not apply, so we are good to start grabbing data
            return true;
        }    
        else if (tagName.equals(subXmlBlock) ) {
            //We are at the suboperation level, next time through we get the tag data
            return true;
        }
        return false;
    }
    
    private boolean markFinished (String endElementName, boolean mainBlock, boolean subBlock) {
        
        // If we need to get data from the main block, but we are not in the main block, we cannot possibly be finished
        if (!mainBlock) {
            return false;
        }
        
        // If we need to get data from the sub block, but we are not in the sub block, we cannot possibly be finished
        if (this.subXmlBlock != null && !subBlock) {
             return false;
        }
        
        if (this.subXmlBlock == null) {
            return endElementName.equals(this.mainOperationXmlBlock);
        }
        else {
            return endElementName.equals(this.subXmlBlock);
        }
    }
}
