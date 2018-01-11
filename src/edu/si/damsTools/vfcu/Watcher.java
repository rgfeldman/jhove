/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu;

import edu.si.damsTools.cdis.Operation;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.deliveryfiles.Md5File;
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
import java.util.HashMap;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import edu.si.damsTools.vfcu.files.xferType.XferTypeFactory;
import edu.si.damsTools.vfcu.database.dbRecords.MediaFileRecord;
import edu.si.damsTools.vfcu.database.dbRecords.BatchFileRecord;


/**
 *
 * @author rfeldman
 */
public class Watcher extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList <Md5File> sourceFileList; 
    
    public Watcher() {
        sourceFileList = new ArrayList();
    }
    
    public void invoke() {  
        
        // If traverse option is selected then we need to obtain a list of md5Files by traversing through the directory tree
        traversePopulateMd5List();
        
        //loop through the files in the md5 master path list
        for (Md5File md5File : sourceFileList) {
        
            BatchFileRecord batchFileRecord = new BatchFileRecord();
            
            // Check to see if md5 file already exists im database
            boolean fileAlreadyProcessed = batchFileRecord.checkIfExistsInDb();
            if (fileAlreadyProcessed) {
                logger.log(Level.FINEST, "File already processed, skipping"); 
                continue;
            }
            
            XferTypeFactory xferTypeFactory = new XferTypeFactory();
            XferType xferType = xferTypeFactory.XferTypeChooser();
            
            // transfer md5 file to staging
            boolean fileXferred = md5File.transferToDAMSStaging(xferType, true);
            if (!fileXferred) {
                logger.log(Level.FINEST, "Error, unable to transfer md5 file to staging"); 
                continue;
            }
            
            // Insert into Database
            boolean recordInserted = batchFileRecord.insertDbRecord();
            //if md5 inserted successfully, add the files too
            if (!recordInserted) {
                logger.log(Level.FINEST, "Error, unable to insert md5 record into database"); 
                continue;
            }
            
            boolean contentMapPopulated = md5File.populateContentsHashMap();
            if (!contentMapPopulated) {
                logger.log(Level.FINEST, "Error, unable to pull contents into HashMap"); 
                continue;
            }
            
            for (String md5Value : md5File.getContentsMap().keySet()) {
                MediaFileRecord mediaFileRecord = new MediaFileRecord();
                mediaFileRecord.getVfcuMediaFile().setVfcuMd5FileId(batchFileRecord.getVfcuMd5File().getVfcuMd5FileId());
                mediaFileRecord.getVfcuMediaFile().setVendorCheckSum(md5Value);
                mediaFileRecord.getVfcuMediaFile().setMediaFileName(md5File.getContentsMap().get(md5Value) );
                boolean dbRecordInserted = mediaFileRecord.getVfcuMediaFile().insertRow();
                if (!dbRecordInserted) {
                    logger.log(Level.FINEST, "Error, unable to insert mediaFile row"); 
                    continue;
                }
             
            }
                         
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
           
        }
 
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }   
    
    public boolean traversePopulateMd5List () {
       
        // walk directory tree starting at the directory specified in config file
        Path startDir = Paths.get(DamsTools.getProperty("vendorBaseDir"));
        logger.log(Level.FINEST, "Starting at: " + DamsTools.getProperty("vendorBaseDir") ); 

        FileSystem fs = FileSystems.getDefault();
        final PathMatcher matcher = fs.getPathMatcher("glob:*.md5");

        FileVisitor<Path> matcherVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path nameAndPath, BasicFileAttributes attribs) {
                Path name = nameAndPath.getFileName();
                if (matcher.matches(name)) {
                    
                   logger.log(Level.FINEST, "Found md5File: " + nameAndPath.toString() ); 
                   Md5File md5File = new Md5File(nameAndPath);
                   sourceFileList.add(md5File);
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
        reqProps.add("vendorBaseDir");
        reqProps.add("vfcuStagingForCDIS");
        reqProps.add("useMasterSubPairs");
        return reqProps;    
    }
}
