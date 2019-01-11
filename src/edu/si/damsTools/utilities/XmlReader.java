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
    
    private XMLEventReader eventReader;
    private XMLInputFactory factory;
    private final String mainXmlBlock;
    private final String subXmlBlock;
    private final String tag;
    private XmlQueryData xmlTagData;
    
    public XmlReader (String mainXmlBlock, String subXmlBlock, String tag) {
        this.mainXmlBlock = mainXmlBlock;
        this.subXmlBlock = subXmlBlock;
        this.tag = tag;
    }
    
    public XmlReader (String mainXmlBlock, String tag) {
        this.mainXmlBlock = mainXmlBlock;
        this.subXmlBlock = null;
        this.tag = tag;
    }
            
    public ArrayList parser() {
      
        String xmlFilename = DamsTools.getProperty(DamsTools.getOperationType() + "-" + DamsTools.getSubOperation() + "XmlFile");
        
        logger.log(Level.FINEST, "Reading xmlFile: " + DamsTools.getDirectoryName() + "/" + xmlFilename);
        String inputfile = DamsTools.getDirectoryName() + "/" + xmlFilename;
        
        ArrayList xmlObjList = new ArrayList();
                
        try {
        factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);

        eventReader = factory.createXMLEventReader(new FileReader(inputfile));
         
        boolean queryInd = false;
        boolean operationInd = false;
        boolean subOperationInd = false;
        boolean finished = false;
        
        while(eventReader.hasNext() && !finished) {
            XMLEvent event = eventReader.nextEvent();
               
            switch(event.getEventType()) {
               
                case XMLStreamConstants.START_ELEMENT:
                    StartElement startElement = event.asStartElement();
                    String qName = startElement.getName().getLocalPart();

                    // If we are already identified as being inside the block we need,
                    //once we set this, we loop through this again and next iteration we get the tag info
                    if (qName.equals(mainXmlBlock)) {
                        operationInd = true;
                        //Loop through again to see if we can set the data
                        continue;
                    }
                   
                    if (operationInd) {
                    //Now that we are in the main block that we need...
                        if (subXmlBlock == null ) {
                            //we have found the tag data we need
                            if (qName.equals(tag)) {
                                queryInd = getAttributeInfo(event);
                            }
                        }
                        else {
                            if (subOperationInd) {
                                if (qName.equals(tag)) {
                                    queryInd = getAttributeInfo(event);
                                }
                            }
                            if (qName.equals(subXmlBlock) ) {
                                //We are at the suboperation level, next time through we get the tag data
                                subOperationInd = true;
                                continue;
                            }
                        }
                    }
                                                  
                break;

                case XMLStreamConstants.CHARACTERS:
                    Characters characters = event.asCharacters();
                    if(queryInd) {          
                        xmlTagData.setDataValue(characters.getData());
                        xmlObjList.add(xmlTagData);    
                        queryInd = false;
                    }
                break;

                case XMLStreamConstants.END_ELEMENT:
                    EndElement endElement = event.asEndElement();
                    finished = markFinished(endElement.getName().getLocalPart(), operationInd, subOperationInd );
                       
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
    
    private boolean getAttributeInfo(XMLEvent event) {
        
        xmlTagData = new XmlQueryData();
        Iterator<Attribute> attributes = event.asStartElement().getAttributes();
        while(attributes.hasNext()){

            Attribute attribute = attributes.next();
            logger.log(Level.FINEST, "Attribute info: " + attribute.getName().toString() + "VALUE " + attribute.getValue());
                            
            xmlTagData.addAttribute(attribute.getName().toString(), attribute.getValue());
        }
        //System.out.println("Type : " + type);
        return true;
    }
    
    private boolean markFinished (String endElementName, boolean mainBlock, boolean subBlock) {
        
        if (!mainBlock) {
            return false;
        }
        
         if (this.subXmlBlock != null && !subBlock) {
             return false;
         }
        
        if (this.subXmlBlock == null) {
            return endElementName.equals(mainXmlBlock);
        }
        else {
            return endElementName.equals(mainXmlBlock);
        }
    }

}
