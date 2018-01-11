/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.files.xferType;

import edu.si.damsTools.DamsTools;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class XferCopy implements XferType {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public String returnCompleteXferCode() {
        return "CV";
    }
    
    public String returnXferErrorCode() {
        return "CFV";
    }
    
    public boolean xferFile(Path source, Path destination) {
        try {
            
            Files.copy(source, destination);
        
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error in copy of media file " + source.toString() +  " to: " + destination.toString(), e);
            return false;
        }
        
        return true;
    }

        
}
