/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.delivery.SourceFileListing;
import edu.si.damsTools.vfcu.database.VfcuMd5FileHierarchy;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import edu.si.damsTools.vfcu.files.xferType.XferTypeFactory;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 * Class: Watcher
 * Description: The Watcher Operation scans the Source Pickup Area for new md5 files that require VFCU processing.
 *              If it finds a new md5 file, it picks up the file, and records the required information in the Database
 */
public class Watcher extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList <SourceFileListing> sourceMasterFileListingArr; 
    private final ArrayList <SourceFileListing> sourceSubFileListingArr; 
    private final ArrayList <SourceFileListing> sourceSubSubFileListingArr;
    private final XferTypeFactory xferTypeFactory;
    private final XferType xferType;
    
    public Watcher() {
        sourceMasterFileListingArr = new ArrayList();
        sourceSubFileListingArr = new ArrayList();
        sourceSubSubFileListingArr = new ArrayList();
        
        xferTypeFactory = new XferTypeFactory();
        xferType = xferTypeFactory.XferTypeChooser();
        
    }
    
    /**
    * Method: invoke
    * Purpose: This Is the main driver method for the Watcher tool
    */
    public void invoke() {  
        
        // We need to obtain a list of md5Files by traversing through the directory tree
        //  Each file we could potentially process is added into a list that we process later on
        traversePopulateSourceBatchList();
        
        //Set the fileTransfer type (move or copy)
 
        //loop through the MASTER files in the md5 master path list
        for (SourceFileListing sourceMasterFileListing : sourceMasterFileListingArr) {
            boolean fileListingRecorded = sourceMasterFileListing.retrieveAndRecord(xferType);
            if (! fileListingRecorded) {
                logger.log(Level.FINEST, "Error, unable to record MasterFile"); 
                continue;
            }
            
            //Populate the FileHierarchy table with the master info
            VfcuMd5FileHierarchy vfcuMd5FileHierarchy = new VfcuMd5FileHierarchy();
            vfcuMd5FileHierarchy.setMasterFileVfcuMd5FileId(sourceMasterFileListing.getVfcuMd5File().getVfcuMd5FileId());
            vfcuMd5FileHierarchy.insertRow();

            
            if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
                //record subFileInfo
                insertSubfileInfo(sourceMasterFileListing, vfcuMd5FileHierarchy);
            }
            

            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
           
        }
 
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }   
    
    /**
    * Method: insertSubfileInfo
    * Purpose: Looks for subfiles, and sub-subfiles for the given MasterFile listing, and inserts that info into the database
    */
    private boolean insertSubfileInfo(SourceFileListing sourceMasterFileListing, VfcuMd5FileHierarchy vfcuMd5FileHierarchy) {
        
        //Now that we have the masterfile recorded in the database, get the subfile record and record that.
        //We do that by comparing the directories.  if the directory one level back is the same, 
        for (SourceFileListing sourceSubFileListing : sourceSubFileListingArr) {
            Path masterPath = sourceMasterFileListing.getMd5File().getFilePathEnding("staging").getParent();
            Path subFilePath = sourceSubFileListing.getMd5File().getFilePathEnding("source").getParent();
                
            logger.log(Level.FINEST, "DEBUG: masterPath " + masterPath.toString()); 
            logger.log(Level.FINEST, "DEBUG: subFilePath " + subFilePath.toString()); 
            
            if (masterPath.equals(subFilePath)) {
                //Paths are the same one level up, record in the table
                boolean fileListingRecorded = sourceSubFileListing.retrieveAndRecord(xferType);
                if (! fileListingRecorded) {
                    logger.log(Level.FINEST, "Error, unable to record SubFile"); 
                    continue;
                }
                //Update the FileHierarchy table by adding this record
                vfcuMd5FileHierarchy.setSubFileVfcuMd5FileId(sourceSubFileListing.getVfcuMd5File().getVfcuMd5FileId());
                vfcuMd5FileHierarchy.updateSubFileVfcuMd5FileId();
            }
        }
            
        //Now that we have the subFile recorded in the database, get the subSubfile record and record that.
        for (SourceFileListing sourceSubSubFileListing : sourceSubSubFileListingArr) {
            Path masterPath = sourceMasterFileListing.getMd5File().getDirectoryPath().getParent();
            Path subFilePath = sourceSubSubFileListing.getMd5File().getDirectoryPath().getParent();
                
            if (masterPath.equals(subFilePath)) {
                //Paths are the same one level up, record in the table
                boolean fileListingRecorded = sourceSubSubFileListing.retrieveAndRecord(xferType);
                if (! fileListingRecorded) {
                    logger.log(Level.FINEST, "Error, unable to record SubSubFile"); 
                    continue;
                }
                //Update the FileHierarchy table by adding this record
                vfcuMd5FileHierarchy.setSubSubFileVfcuMd5FileId(sourceSubSubFileListing.getVfcuMd5File().getVfcuMd5FileId());
                vfcuMd5FileHierarchy.updateSubSubFileVfcuMd5FileId();
            }
        }    
        return true;
    }
    
    /**
    * Method: populateFileListingArr
    * Description: Populates the filename and path information into one of three arrays. 
    *               Each array representing a different file hierarchy level in DAMS (master/subfile/subSubfile)
    */
    public void populateFileListingArr(Path nameAndPath) {
        
        //If we are not using master/subfile pairs, we set all files to master
        if (DamsTools.getProperty("useMasterSubPairs").equals("false")) {
            SourceFileListing sourceFileListing = new SourceFileListing();
            sourceFileListing.populateBasicValuesFromDeliveryFile(nameAndPath);
            this.sourceMasterFileListingArr.add(sourceFileListing);
            return;
        }
        
        String lastDirLevel = nameAndPath.getParent().getFileName().toString();
        if ( lastDirLevel.equals(DamsTools.getProperty("masterFileDir"))) {
            SourceFileListing sourceFileListing = new SourceFileListing();
            sourceFileListing.populateBasicValuesFromDeliveryFile(nameAndPath);
            this.sourceMasterFileListingArr.add(sourceFileListing);
        }
        else if (lastDirLevel.equals(DamsTools.getProperty("subFileDir"))) {
            SourceFileListing sourceFileListing = new SourceFileListing();
            sourceFileListing.populateBasicValuesFromDeliveryFile(nameAndPath);
            this.sourceSubFileListingArr.add(sourceFileListing);
        }
        else if (lastDirLevel.equals(DamsTools.getProperty("subSubFileDir"))) {
            SourceFileListing sourceFileListing = new SourceFileListing();
            sourceFileListing.populateBasicValuesFromDeliveryFile(nameAndPath);
            this.sourceSubSubFileListingArr.add(sourceFileListing);
        }
    }
    
    /**
    * Method: traversePopulateSourceBatchList
    * Description: obtain a list of md5Files by traversing through the directory tree
    *  Each file we could potentially process is added into a list that we process later on
    */
    public boolean traversePopulateSourceBatchList () {
       
        // walk directory tree starting at the directory specified in config file
        Path startDir = Paths.get(DamsTools.getProperty("sourceBaseDir"));
        logger.log(Level.FINEST, "Starting at: " + DamsTools.getProperty("sourceBaseDir") ); 

        FileSystem fs = FileSystems.getDefault();
        //we are only interested in picking up md5 files
        final PathMatcher matcher = fs.getPathMatcher("glob:*.md5");

        FileVisitor<Path> matcherVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path nameAndPath, BasicFileAttributes attribs) { 
                Path name = nameAndPath.getFileName();
                if (matcher.matches(name)) {
                    
                    //If we find a file, we add it to a list of md5 files that we need to potentially add
                    logger.log(Level.FINEST, "Found md5File: " + nameAndPath.toString() ); 
                    populateFileListingArr(nameAndPath);    
                }
                return FileVisitResult.CONTINUE;
            }
        }; 
        
        try {
            EnumSet fileVisitOptions = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
            Files.walkFileTree(startDir, fileVisitOptions, 12, matcherVisitor);
            
        } catch (Exception e) {
            logger.log(Level.FINEST, "Exception ", e );
        }
        
        return true;        
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        reqProps.add("sourceBaseDir");
        reqProps.add("vfcuStaging");
        reqProps.add("useMasterSubPairs");
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            reqProps.add("masterFileDir");
            reqProps.add("subFileDir");
        }
        return reqProps;    
    }
}
