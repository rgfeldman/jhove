/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.CDISActivityLog;
import edu.si.CDIS.DAMS.Database.CDISMap;
import edu.si.CDIS.utilties.ErrorLog;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import java.sql.Connection;

/**
 *
 * @author rfeldman
 */
public class HotIngestFolder {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public ArrayList<String> distinctHotFolders;
    String baseDir;
    String folderLetterExtension;
    
    public void setBaseDir (String baseDir) {
        this.baseDir = baseDir;
    }
    
    public boolean populateDistinctList(Connection damsConn, Long batchNumber) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        this.distinctHotFolders = new ArrayList<String>();
        
        String sql = "SELECT    distinct a.hot_folder " + 
                    "FROM       cdis_for_ingest a, " +
                    "           cdis_map b " +
                    "WHERE  a.cis_unique_media_id = b.cis_unique_media_id " +
                    "AND    a.si_holding_unit = b.si_holding_unit " +
                    "AND    batch_number = " + batchNumber;
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs != null && rs.next()) {
                distinctHotFolders.add(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        return true;
        
    }
    
    //Counts the files in the hotfolders
    public Integer countIngestFiles(Long batchNumber, String folderType) {
    
        // Check to see if anything is in hotfolder.  If there is, we need to wait for hotfolder to clear or we can run the risk of 
        // having files being ingested that are partially there.
            
        Integer numFiles = 0;
        
        for(Iterator<String> iter = distinctHotFolders.iterator(); iter.hasNext();) {
            
            String folderLocation = null;
            
            if (folderType.equals("workFolder")) {
                folderLocation = this.baseDir + "\\" + iter.next() + "a\\TEMP-XFER\\" + batchNumber;
            }
            else if (folderType.equals("master")) {
                folderLocation = this.baseDir + "\\" + iter.next() + this.folderLetterExtension + "\\MASTER";
            }
            else {
                logger.log(Level.FINER, "Error, invalid folder type");
                return -1;
            }
            
            File hotFolderSubDir = new File(folderLocation);
            
            if(hotFolderSubDir.isDirectory()){ 
                numFiles += hotFolderSubDir.list().length;
                logger.log(Level.FINER, "Directory: " + folderLocation + " Files: " + numFiles);
            }
            else {
                logger.log(Level.FINER, "Error, directory does not exist: " + folderLocation);
                return -1;
            }
        }
        
        return numFiles;
        
    }
    
    
    public void sendTo (Connection damsConn, Long batchNumber) {
          
        Integer numFilesinCurrentFolder;
        
        // We need to look in all workFolders for this batch 
        for(Iterator<String> iter = distinctHotFolders.iterator(); iter.hasNext();) {
            
            numFilesinCurrentFolder = 0;
            
            String currentMainDirName = iter.next();
            
            //Set the hotfolder letter extension for multi-processing.
            //First we start with the letter 'a' then increment
            folderLetterExtension = "a";

            // Check to see if hotfolder with letter extension has data in it
            // If it does, then increment the letter extension
                               
            Integer numFiles;
            boolean firstPass = true;
            File currentHotFolderMaster;
            
            do {
                
                currentHotFolderMaster = new File (baseDir + "\\" + currentMainDirName + folderLetterExtension + "\\MASTER"); 
                numFiles = countIngestFiles (batchNumber, "master");
               
                if (numFiles == -1 ) {
                    //The direcotry does not exist
                    if (firstPass) {
                        //invalid, hot folder does not exist. Return
                        logger.log(Level.FINER, "Invalid HotFolderName, cannot ingest: " + currentHotFolderMaster.getAbsolutePath());
                        return;
                    }
                    else {
                        //HotFolder extension does not exist...but previous ones did.
                        //Set to "a" and start back at beginning
                        logger.log(Level.FINER, "hotFolder Master Directory is not empty.  Will sleep and check back in 5 minutes");
                
                        try {
                            Thread.sleep(300000);
                        } catch (Exception e) {
                                logger.log(Level.FINER, "Exception in sleep ", e);
                        }
            
                        folderLetterExtension = "a"; 
                        
                    }
                }
                else if ( numFiles > 0 ) {
                    int charValue = folderLetterExtension.charAt(0);
                    folderLetterExtension = String.valueOf( (char) (charValue + 1));
                    firstPass = false;
                }   

            } while (numFiles != 0);
               

            String currentHotFolderMasterName = currentHotFolderMaster.getAbsolutePath();
            logger.log(Level.FINER, "Current hotfolder set to : " + currentHotFolderMasterName);
            
            //Set the Workfile vatiable information
            String batchWorkFileDirName = baseDir + "\\" + currentMainDirName + "a\\TEMP-XFER\\" + batchNumber;
            File batchWorkFileDir = new File(batchWorkFileDirName);
            logger.log(Level.FINER, "Current workfolder set to : " + batchWorkFileDirName);
            
            // For each file in current work folder, move to the hot folder MASTER directory
            File[] filesForDams = batchWorkFileDir.listFiles();
            
            for(int i = 0; i < filesForDams.length; i++) {
            
                //Get MapID for logging and error capturing
                CDISMap cdisMap =  new CDISMap();
                cdisMap.setBatchNumber(batchNumber);
                cdisMap.setFileName(filesForDams[i].getName()); 
                cdisMap.populateIDForFileBatch(damsConn);
                
                try {
                    
                    //skip the file if named Thumbs.db
                    if (cdisMap.getFileName().equals("Thumbs.db")) {
                        continue;
                    }
                    
                    File fileForDams = filesForDams[i];
                     
                    if (! currentHotFolderMaster.exists()) {
                        logger.log(Level.FINER, "Error, hot folder not found : " + currentHotFolderMasterName);
                        continue;
                    }
                    FileUtils.moveFileToDirectory(fileForDams, currentHotFolderMaster, false);
                     
                    CDISActivityLog cdisActivity = new CDISActivityLog();
                    cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                    cdisActivity.setCdisStatusCd("SH");   
                    boolean activityLogged = cdisActivity.insertActivity(damsConn);
                    if (!activityLogged) {
                            logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                            continue;
                    } 
                    
                    //if we got this far we have succeeeded, increment count of files in hotfolder...
                    numFilesinCurrentFolder ++;
                    
                } catch (Exception e) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "MFH", "Move to Hotfolder MASTER Failure for FileName:" + cdisMap.getFileName()  + " " + e, damsConn );
                } 
            }
                         
            // If the processing was successful, files have been moved out and the directory will be empty
            // delete the workfolder subdirectory if it is empty 
            filesForDams = batchWorkFileDir.listFiles();
            if (! (filesForDams.length > 0)) { 
                logger.log(Level.FINER, "Deleting empty work file");
                batchWorkFileDir.delete();
            }  
            
        if (numFilesinCurrentFolder > 0) {
            //create ready file in current hotfolder    
            createReadyFile(currentHotFolderMasterName);        }
        }
    }
    
    /*  Method :        createReadyFile
        Arguments:      
        Returns:      
        Description:    Creates empty file named 'ready.txt' in hot folder.
                        This file indicates for the DAMS to create images based on the files in the hotfolder location
        RFeldman 3/2015
    */
    
    private void createReadyFile (String hotDirectoryName) {
    
        String readyFilewithPath = null;
        
        try {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = hotDirectoryName + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }
    
}
