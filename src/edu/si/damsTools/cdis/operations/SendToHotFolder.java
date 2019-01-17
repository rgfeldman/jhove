/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import edu.si.damsTools.cdis.dams.StagedFile;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.cdis.dams.HotIngestFolder;

import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.Folders;
import edu.si.damsTools.utilities.XmlData;
import edu.si.damsTools.utilities.XmlUtils;
import edu.si.damsTools.vfcu.delivery.MediaFileRecord;

/**
 *
 * @author rfeldman
 */
public class SendToHotFolder extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList <MediaFileRecord> masterMediaFileRecords; 
    private final HotIngestFolder hotIngestFolder;
            
    public SendToHotFolder() {
        masterMediaFileRecords = new ArrayList();
        hotIngestFolder = new HotIngestFolder();
    }
    
    
    private void createReadyFile () {
        
        try {
            
            Files.createFile(hotIngestFolder.returnMasterPath().resolve("ready.txt"));
            
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);
        }
    }   
        
    private boolean logChildRecord (Integer parentVfcuMediaFileId) {
        
        VfcuMediaFile vfcuMediafile = new VfcuMediaFile();
        vfcuMediafile.setVfcuMediaFileId(parentVfcuMediaFileId);
            
        // Get the child record
        int childVfcuMediaFileId = vfcuMediafile.retrieveSubFileId();
        if (! (childVfcuMediaFileId > 0 )) {
            logger.log(Level.FINER, "Could not get child ID");
            return false;
        }
        
        vfcuMediafile.setVfcuMediaFileId(childVfcuMediaFileId);

        vfcuMediafile.populateMediaFileName();
                    
        CdisMap cdisMap = new CdisMap();
        cdisMap.setFileName(vfcuMediafile.getMediaFileName());
        cdisMap.setVfcuMediaFileId(vfcuMediafile.getVfcuMediaFileId());
        
        // put the entry into the CDIS_MAP table
        boolean mapEntryCreated = cdisMap.createRecord();
            
        if (!mapEntryCreated) { 
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
            return false;
        }
             
        return true;
    }
    
    private void recordCdisMap () {
        
        for (MediaFileRecord mediaFileRecord: masterMediaFileRecords ) {
            
            //Make sure the last transaction is committed
            try { if ( DamsTools.getDamsConn()!= null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            logger.log(Level.FINEST, "Processing for vfcu_meda_file_id: " + mediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId());
              
            /*Create CDIS Map Entry*/
            CdisMap cdisMap = new CdisMap();                           
            cdisMap.setFileName(mediaFileRecord.getVfcuMediaFile().getMediaFileName());
            cdisMap.setVfcuMediaFileId(mediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId());
            
             // Now that we have the cdismap info, Add the media to the CDIS_MAP table
            boolean mapEntryCreated = cdisMap.createRecord();          
            if (!mapEntryCreated) {
                cdisMap.setCdisMapId(null);
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
                
                masterMediaFileRecords.removeIf(mapNotCreated -> true);
                continue;
            }
            
            //Take care of any child file attached to this master
            if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true") ) {
                boolean childProcessed = logChildRecord(cdisMap.getVfcuMediaFileId());
                
                masterMediaFileRecords.removeIf(mapNotCreated -> !childProcessed);
                
            }            
        }
        
        //commit so the last record is saved for this batch so another batch doesnt pick it up
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }      
    }

    
    /*  Method :        populateFileListToIngest  
        Description:    Adds to the list of media IDs that need to be integrated into DAMS
    */
    private boolean populateVfcuListToIngest () {
        
        int recordCount;
        String sql = XmlUtils.returnFirstSqlForTag("idListToSend");
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            double fileSizeSoFar = 0L;
            
            //Add the value from the database to the list, as long as we are below the max number of records
            for (recordCount =1; 
                    rs.next() && recordCount <= Integer.parseInt(XmlUtils.getConfigValue("maxNumMasterFiles")); 
                    recordCount ++ ) {
                        
                MediaFileRecord mediaFileRecord = new MediaFileRecord(rs.getInt(1));
                mediaFileRecord.populateBasicValuesFromDb();
                 
                //Now keep track of the total filesize we are using
                mediaFileRecord.getVfcuMediaFile().populateMediaFileAttr();
                fileSizeSoFar = fileSizeSoFar + mediaFileRecord.getVfcuMediaFile().getMbFileSize();
                
                masterMediaFileRecords.add(mediaFileRecord);
                
                if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true") ) {
                    VfcuMediaFile childVfcuMediaFile = new VfcuMediaFile();
                    childVfcuMediaFile.setVfcuMediaFileId(mediaFileRecord.getVfcuMediaFile().getChildVfcuMediaFileId());
                    childVfcuMediaFile.populateMediaFileAttr();
                    fileSizeSoFar = fileSizeSoFar + childVfcuMediaFile.getMbFileSize();     
                }
                
                if (fileSizeSoFar > Double.parseDouble(XmlUtils.getConfigValue("maxBatchMbSize")) ) {
                    logger.log(Level.FINEST, "Ending batch size early, batch size is too large to continue");
                    logger.log(Level.FINEST, "Current size: " + fileSizeSoFar + " Max Size: " + XmlUtils.getConfigValue("maxBatchMbSize") );
                    break;
                }              
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
            return false;
        }
        
        return true;        
    }
    
    /*  Method :        invoke
        Description:    The main entrypoint or 'driver' for the ingestToDams operation Type
    */
     public void invoke () {
        
        // Get the list of new Media to add to DAMS
        boolean masterListGenerated = populateVfcuListToIngest ();
        if  (! masterListGenerated || masterMediaFileRecords.isEmpty()) {
             logger.log(Level.FINER, "No Media Found to process in this batch.  Exiting");
             return;
        }
        
        //Before we transfer files, we need to reserve a hot folder.  We cannot run multiple instances of this at the same time
        //for the same hotfolder, or we can get collisions/bad results!
        hotIngestFolder.setNextAvailable();
        
        //Make sure we have found the hotfolder
        if (hotIngestFolder.getBasePath()== null ){
             logger.log(Level.FINER, "Unable to obtain hotfolder, cannot send any files");
             return;
        }
                
        // Process each media item from list (move to workfolder/hotfolder)
        logger.log(Level.FINER, "Processing media List");
        recordCdisMap();
        
        xferFilesToHotFolder ();
        
        //See if any files were transferred
        int numMasterFiles = Folders.returnCount(hotIngestFolder.returnMasterPath());
        
        if  (! (numMasterFiles > 0)) {   
            logger.log(Level.FINER, "Do not continue, no files were put in MASTER location");
            return;
        }    
               
        //Perform an extra validation, make sure the number of files in MASTER is the same as the number of files in SUBFILE
        if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true") ) {
            if  (numMasterFiles !=  Folders.returnCount(hotIngestFolder.returnSubfilePath()) ) {
                logger.log(Level.FINER, "Do not continue, number of subfiles != number of master files");
                return;
            }      
        }      
        //if we have gotten this far, create the ready.txt file
        createReadyFile();
        
     }
     

    private boolean xferFilesToHotFolder () {
            
        //loop through the NotLinked Master Media and transfer the files
        for (MediaFileRecord mediaFileRecord : masterMediaFileRecords  ) {       
            
            try {
                
                StagedFile stagedFile = new StagedFile();
                
                CdisMap masterCdisMap = new CdisMap();
                masterCdisMap.setVfcuMediaFileId(mediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId());
                masterCdisMap.populateIdFromVfcuId();
                
                try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
                if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true") )  {
                    
                    CdisMap childCdisMap = new CdisMap();
                    childCdisMap.setVfcuMediaFileId(mediaFileRecord.getVfcuMediaFile().getChildVfcuMediaFileId());
                    childCdisMap.populateIdFromVfcuId();
                    
                    //send subfile to hotfolder first             
                    Integer subFileId = mediaFileRecord.getVfcuMediaFile().getChildVfcuMediaFileId();
                    MediaFileRecord subFileMediaRecord = new MediaFileRecord(subFileId); 
                    subFileMediaRecord.populateBasicValuesFromDb();
                    
                    //Get the file path for the vfcu_id
                    boolean infoPopulated = stagedFile.populateNameStagingPathFromId(subFileMediaRecord.getVfcuMediaFile().getVfcuMediaFileId());
                    if (! infoPopulated) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(childCdisMap, "XFHOTF ", "Error, unable to populate name and path from database for subfile ");
                        continue;
                    }
                    
                    logger.log(Level.FINER, "MediaFile xfer: " + subFileMediaRecord.getVfcuMediaFile().getMediaFileName() );
                    boolean fileXferred = stagedFile.xferToHotFolder(hotIngestFolder.returnSubfilePath()); 
                    
                      
                    if (! fileXferred) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(childCdisMap, "XFHOTF", "Error, unable to copy file to subfile: " + stagedFile.getFileName());
                        continue;
                    }
                
                    CdisActivityLog cdisActivity = new CdisActivityLog();
                    cdisActivity.setCdisMapId(childCdisMap.getCdisMapId());
                    cdisActivity.setCdisStatusCd("FXS");
                    boolean activityLogged = cdisActivity.insertActivity();
                    if (!activityLogged) {
                        logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                        continue;
                    }
                }
                
                //now populate information needed for the master
                boolean infoPopulated = stagedFile.populateNameStagingPathFromId(mediaFileRecord.getVfcuMediaFile().getVfcuMediaFileId() );
                if (! infoPopulated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(masterCdisMap, "MVHOTF", "Error, unable to populate name and path from database for master file ");
                    continue;
                }
                
                //Decide move or to copy master file
                boolean fileMoved = stagedFile.xferToHotFolder(hotIngestFolder.returnMasterPath());  
                if (! fileMoved) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(masterCdisMap, "MVHOTF", "Error, unable to move file to master: " + stagedFile.getFileName());
                    continue;
                }
                
                CdisActivityLog cdisActivity = new CdisActivityLog();
                cdisActivity.setCdisMapId(masterCdisMap.getCdisMapId());   
                cdisActivity.setCdisStatusCd("FMM");
                boolean activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }
                            
            }  finally {
                
                //make sure we commit the final time through the loop
                try { if (DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
            }
        }
        return true;
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("failedFolderArea");
        reqProps.add("failedIngestThreshold");
        reqProps.add("hotFolderArea");
        reqProps.add("maxBatchMbSize");
        reqProps.add("maxNumMasterFiles");
        reqProps.add("maxHotFolderIncrement");
        reqProps.add("retainAfterIngest");
        reqProps.add("useMasterSubPairs");
        return reqProps;    
    }
    
    public boolean requireSqlCriteria () {
        return true;
    }

}
