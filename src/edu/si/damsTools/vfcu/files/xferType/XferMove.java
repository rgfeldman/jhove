/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.files.xferType;

import edu.si.damsTools.DamsTools;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
/**
 *
 * @author rfeldman
 */
public class XferMove implements XferType {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    private String failureMessage;
    
    public String returnCompleteXferCode() {
        return "MV";
    }
    
    public String returnFailureMessage() {
        return this.failureMessage;
    }
    
    public String returnXferErrorCode() {
        return "MFV";
    }
    
    public boolean xferFile(Path source, Path destination) {
        try {
            
            Files.move(source, destination);
            
        } catch (AccessDeniedException e ) {
            failureMessage = "File permissions/access denied";
            return false;
        } catch (FileAlreadyExistsException e) {
            failureMessage = "File already exists in VFCU staging";
            return false;
        } catch (NoSuchFileException e) {
            failureMessage = "File listed in md5 listing, but not found in pickup location";
            return false;
        } catch (Exception e) {
            failureMessage = "Error in move of media file ";
            logger.log(Level.FINEST, "Error in move of media file " + source.toString() +  " to: " + destination.toString(), e);
            return false;
        }
        return true;        
    }
}
