/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.vfcu.files.VendorMd5File;
import edu.si.damsTools.vfcu.database.VFCUMd5File;
import java.nio.file.Files;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.EnumSet;
import java.nio.file.FileVisitOption;
import edu.si.damsTools.DamsTools;

/**
 *
 * @author rfeldman
 */
public class Watcher {

    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());  
    
    public ArrayList <String> masterPaths; 
    
    
    private void locateMasterIdsForSubfiles() {
        
        
        // Find any rows where the master md5 ID is null, these are subfiles.
        // We need to match up these subfiles with master IDs if we can find them.
        ArrayList<Integer> masterlessMd5Ids = new ArrayList<> ();
        
        VFCUMd5File vfcuMd5File = new VFCUMd5File();
        masterlessMd5Ids = vfcuMd5File.returnMd5sWithNoMasterID();
        
        for (Integer masterlessMd5Id : masterlessMd5Ids) {

            vfcuMd5File.setVfcuMd5FileId(masterlessMd5Id);
            
            // Retrieve path information for this md5 fileID
            boolean pathPopulated = vfcuMd5File.populateVendorFilePath();
            
            if (pathPopulated) {
                // Find a row for the same base path, and the same filepath ending with the SubfileDir substituted with the masterDir
                String filePathEnding = vfcuMd5File.getFilePathEnding();
                
                if (filePathEnding.endsWith(DamsTools.getProperty("vendorSubFileDir")) ){
                    //Strip off the last level of the directory from the filePathEnding
                    String masterFilePath =  filePathEnding.substring(0,filePathEnding.lastIndexOf('/') + 1) + DamsTools.getProperty("vendorMasterFileDir");
                    vfcuMd5File.setFilePathEnding(masterFilePath);
              
                    //locate the md5Id for the basePath and FileEnding
                    Integer masterId = vfcuMd5File.returnMasterIdForPath();
                    
                    //If the masterID was found, update the vfcu table with the masterID
                    if (masterId != null) {
                        vfcuMd5File.setMasterMd5FileId(masterId);
                       
                        //update MasterId in Table
                        vfcuMd5File.updateMasterMd5File_id();
                    }
                }   
            }
        }
        
    }
     
    public boolean traversePopulatePathList () {
        
        this.masterPaths = new ArrayList <>();
        
        //walk directory tree starting at the directory specified in config file
        Path startDir = Paths.get(DamsTools.getProperty("vendorBaseDir"));
        logger.log(Level.FINEST, "Starting at: " + DamsTools.getProperty("vendorBaseDir") ); 

        FileSystem fs = FileSystems.getDefault();
        final PathMatcher matcher = fs.getPathMatcher("glob:*.md5");

        FileVisitor<Path> matcherVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attribs) {
                Path name = file.getFileName();
                if (matcher.matches(name)) {
                    
                    logger.log(Level.FINEST, "Found md5File: " + file.toString() ); 
                    masterPaths.add(file.toString());
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
    
    
    public void watchForNewMd5() {  
     
        VendorMd5File md5File = new VendorMd5File();
        md5File.setBasePathVendor(DamsTools.getProperty("vendorBaseDir"));
        md5File.setBasePathStaging(DamsTools.getProperty("vfcuStagingForCDIS"));
        
        // If traverse option is selected then we need to obtain a list of md5Files by traversing through the directory tree
        traversePopulatePathList();
        
        //loop through the files in the md5 master path list
        for (String fileAndPathName : masterPaths) {
            
            String filePathEnding = fileAndPathName.substring(md5File.getBasePathVendor().length() ,fileAndPathName.lastIndexOf('/') );
            //chop off initial '\' if it is there
            filePathEnding = filePathEnding.startsWith("/") ? filePathEnding.substring(1) : filePathEnding;
            md5File.setFilePathEnding(filePathEnding);
                    
            // Get the md5FileName in seperate variable for easy insert into Database
            String md5FileName = fileAndPathName.substring(fileAndPathName.lastIndexOf('/') + 1);   
            md5File.setFileName(md5FileName);
        
            md5File.recordInfoInDB();
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
           
        }
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true") ) {
            locateMasterIdsForSubfiles();
        }
        
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
   }   
}
