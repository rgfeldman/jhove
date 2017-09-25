/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools;

import java.util.ArrayList;
import edu.si.damsTools.vfcu.Report;
import edu.si.damsTools.vfcu.Watcher;
import edu.si.damsTools.vfcu.VendorFileCopy;
import edu.si.damsTools.cdis.Operation;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 * @author rfeldman
 */
public class Vfcu extends App {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public Operation operationFactiory() {
        
        Operation operation = null;
        
        switch (DamsTools.getOperationType()) {
            
            case "report" :  
                operation = new Report();       
                break;
            
            case "watcher" :
                operation = new Watcher();
                break;
            
            case "copyValidate" :      
                operation = new VendorFileCopy();
                break;
                    
            default:     
                logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting"); 
        }
        
        return operation;
    }
}
