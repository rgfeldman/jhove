/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.Folders;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */

public class HotIngestFolder {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    Path basePath;
    
    public Path getBasePath() {
        return this.basePath;
    }
    
    public Path returnMasterPath() {
        return basePath.resolve("MASTER");
    }
    
    public Path returnSubfilePath() {
        return basePath.resolve("SUBFILES");
        
    }
    
    /*  Method : returnAssociatedFailedPath
      Description:  returns the path of the failed file location for the current hotfolder
    */       
    public Path returnAssociatedFailedPath() {

        Path failedPath = null;
                
        try {
                 
            if (DamsTools.getProperty("maxHotFolderIncrement").equals("0")) {
                 failedPath = Paths.get(DamsTools.getProperty("failedFolderArea"));
            }
            else {              
                failedPath = Paths.get(DamsTools.getProperty("failedFolderArea") + 
                        basePath.toString().substring(basePath.toString().lastIndexOf("_") ));
            }  
            failedPath = failedPath.resolve("FAILED");
                   
        } catch (Exception e) {
            logger.log(Level.FINER, "Invalid or not-existing failed path " + basePath);
            return null;
        }
        
        return failedPath;

    }
    
    /*  Method :        setValidateBasePath
        Description:    sets the basepath, and validates that it conforms to hotfolder specifications  
    */
    private void setValidateBasePath(String basePathLocation)  {
        
        try {
            //Sets the basepath and makes sure it is there
            Path bp = Paths.get(basePathLocation);
            
            logger.log(Level.FINER, "Master Path " + basePathLocation);
            
            //Make sure the basepath has a master and subfiles subdirectory
            if (Files.exists(bp.resolve("MASTER")) && Files.exists(bp.resolve("SUBFILES"))) { 
                this.basePath = bp;    
            }
            else {
                throw new Exception();
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Invalid or not-existing path " + basePathLocation);
            this.basePath = null;
        }
    }
    
    public boolean setNextAvailable() {
        //Obtain empty hot folder to put these files into

        int maxHotFolderIncrement;
        
        if (DamsTools.getProperty("maxHotFolderIncrement").equals("0")) {
            maxHotFolderIncrement = 1;
        }
        else {
            maxHotFolderIncrement = Integer.parseInt(DamsTools.getProperty("maxHotFolderIncrement"));
        }
           
        for (int currentFolderIncrement = 1;; currentFolderIncrement ++) {
            
            if (currentFolderIncrement > maxHotFolderIncrement ) {
                //We exceeded the number of hotfolders allocated for this project.
                try {
                    logger.log(Level.FINER, "Sleeping, waiting for available hot folder...");
                    Thread.sleep(300000);  //sleep 5 minutes
                    currentFolderIncrement = 0;
                    continue;
                } catch (Exception e) {
                    logger.log(Level.FINER, "Exception in Thread.sleep: " + this.basePath.toString());
                }               
            }
            
            if (DamsTools.getProperty("maxHotFolderIncrement").equals("0")) {
                //If we have a '0' in the configuration file, we do not use incrementally named hotfolders
                // There is no increment, we do not append any number to the hot folder name
                setValidateBasePath(DamsTools.getProperty("hotFolderArea"));
                
            } else {
                // We use incremental hot folder names
                setValidateBasePath(DamsTools.getProperty("hotFolderArea") + "_" + currentFolderIncrement);
            }
            
            if (getBasePath() == null) {
                //return, we do not have a valid hot folder location
                logger.log(Level.FINER, "Error, no bastpath located ");
                return false;
            }
            
            //See if the hotfolder is empty
            boolean folderIsEmpty = Folders.isEmpty(this.returnMasterPath() ) &&  Folders.isEmpty(this.returnSubfilePath()) ;
            if (!folderIsEmpty) {
                //Folder is full, Try the next hotfolder in the sequence
                continue;
            }

            //See if the number of failed files in the failed area is over the provided threshold.
            // If the number of failed files is OVER the threshold, we keep looping until we no longer have that condition
            int numFailedFiles = Folders.returnCount(this.returnAssociatedFailedPath());
            if (numFailedFiles < Integer.parseInt(DamsTools.getProperty("failedIngestThreshold") )) {
                //We have a hotfolder selected
                break;
            }
            else {
                logger.log(Level.FINER, "WARNING: Failure Threshold exceeded for hotfolder" + this.basePath.toString());
            }
        }
        
        logger.log(Level.FINER, "Will use current hotfolder: " + this.basePath.toString());
        return true;   
    }
}
