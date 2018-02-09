/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools;

import edu.si.damsTools.cdis.operations.CISThumbnailSync;
import edu.si.damsTools.cdis.operations.CisUpdate;
import edu.si.damsTools.cdis.operations.CreateCisMedia;
import edu.si.damsTools.cdis.operations.LinkCisRecord;
import edu.si.damsTools.cdis.operations.LinkDamsRecord;
import edu.si.damsTools.cdis.operations.MetaDataSyncOld;
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.cdis.operations.ReportGenerator;
import edu.si.damsTools.cdis.operations.SendToHotFolder;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class Cdis extends App {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public Operation operationFactiory() {
        
        Operation operation = null;
                
        switch (DamsTools.getOperationType()) {
            
            case "sendToHotFolder" :
                operation = new SendToHotFolder();
                break;
                            
             case "linkCisRecord" :
                operation = new LinkCisRecord();
                break;
                
             case "linkDamsRecord" :           
                operation = new LinkDamsRecord();
                break;

            case "metadataSync" :    
                operation = new MetaDataSyncOld();
                break;
                    
            case "createCisMedia" :
                operation = new CreateCisMedia();
                break;  
                    
            case "thumbnailSync" :    
                operation = new CISThumbnailSync();
                break;
                    
            case "cisUpdate" :
                operation = new CisUpdate();
                break;
                    
            case "vfcuDirReport" :
            case "timeframeReport" :    
                operation = new ReportGenerator();
                break;
 
            default:     
                logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting"); 
        }
        
        return operation;
            
    }

}
