/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools;

import edu.si.damsTools.vfcu.Report;
import edu.si.damsTools.vfcu.Watcher; 
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.vfcu.MediaPickupValidate;
import edu.si.damsTools.vfcu.BatchCompletion;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class Vfcu extends App {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public Operation operationFactiory() {
        
        Operation operation = null;
        
        switch (DamsTools.getOperationType()) {
            
            case "batchCompletion" :      
                operation = new BatchCompletion();
                break;
                
            case "copyValidate" :      
                operation = new MediaPickupValidate();
                break;
            
            case "report" :  
                operation = new Report();       
                break;
            
            case "watcher" :
                operation = new Watcher();
                break;
         
            default:     
                logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting"); 
        }
        
        return operation;
    }
}
