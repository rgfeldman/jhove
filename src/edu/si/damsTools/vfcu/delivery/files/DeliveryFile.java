/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.delivery.files;

/**
 * Class : DeliveryFile.java
 * Purpose: This Class holds the common methods of all types of files that are delivered to VFCU from the Unit/Vendor
 * @author rfeldman
 */
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeliveryFile {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    Path fileNameAndPath;

    public DeliveryFile(Path sourceNameAndPath) {
        this.fileNameAndPath = sourceNameAndPath;
    }
    
    public Path getFileNameWithPath() {
        return fileNameAndPath;
    }
        
    public Path getDirectoryPath() {
        return fileNameAndPath.getParent();
    }
    
    public Path getFileName() {
        return fileNameAndPath.getFileName();
    }
    
    //Gets the filepathending from the file location when the file is in staging
    public Path getFilePathEnding(String locationArea) {

        Path filePathEnding = Paths.get("");
        logger.log(Level.FINEST, "fileNameAndPath: " + fileNameAndPath.toString());
        
        int vfcuBaseCount = 0;
        
        switch (locationArea) {            
            case "source" :
                vfcuBaseCount = Paths.get(DamsTools.getProperty("sourceBaseDir")).getNameCount();
                break;
            case "staging" :
                vfcuBaseCount = Paths.get(DamsTools.getProperty("vfcuStaging")).getNameCount();
        }
        
        int fileNameAndPathCount = fileNameAndPath.getNameCount();
        
        //The sourceName and path can have NO localPathEnding.  This is valid condition, and in that case the localPathEnding is an empty string
        //This occurs when VFCU is pointed directly at a particular directory.  We check to make sure this is not an empty string first or the substr may fail.
        if (fileNameAndPath.getNameCount() > vfcuBaseCount + 1 )  {
            filePathEnding = fileNameAndPath.subpath(vfcuBaseCount, fileNameAndPathCount -1);
            logger.log(Level.FINEST, "filePathEnding: " + filePathEnding);
        }   
        
        return filePathEnding;
    }
    
    
    /*
    /* Method: transferToVfcuStaging
    /* Purpose: Transfers any file from VFCU source area to Vfcu Staging 
    */
    public boolean transferToVfcuStaging(XferType xferType, boolean createMissingDir) {

        Path destinationDir = Paths.get(DamsTools.getProperty("vfcuStaging"));
        if ( getFilePathEnding("source").getNameCount() > 0) {
            destinationDir = destinationDir.resolve(getFilePathEnding("source"));
        }
        
        logger.log(Level.FINEST, "Source: " + fileNameAndPath);
        logger.log(Level.FINEST, "Destination " + destinationDir.toString());
       
        try {
            if (createMissingDir) {
           
                Path newDirectory = destinationDir;  
                logger.log(Level.FINEST, "New Folder  " + destinationDir.toString());
             
                Files.createDirectories(newDirectory);
            }
           
            boolean fileXfered = xferType.xferFile(fileNameAndPath, destinationDir.resolve(getFileName()));
                if (!fileXfered) {
                return false;
            }
            
            //Now that we have moved the file, our file of reference is the file we just moved.  point file to the destination area
            this.fileNameAndPath = destinationDir.resolve(getFileName());
        }
        catch(Exception e) {
             logger.log(Level.FINEST, "Unable to transfer to staging", e);
             return false;
        }
        return true;
    }
}
