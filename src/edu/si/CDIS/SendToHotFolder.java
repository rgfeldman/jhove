/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.DAMS.StagedFile;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.MediaTypeConfigR;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.VFCUMediaFile;
import edu.si.CDIS.utilties.ErrorLog;
import edu.si.Utils.XmlSqlConfig;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class SendToHotFolder {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    LinkedHashMap <String,String> masterMediaIds; 
    
    private String hotFolderBaseName;
    private String fullMasterHotFolderNm;
    private File fullMasterHotFolder;
    
    
    private void createReadyFile (String hotDirectoryName) {
        
        try {
                //Create the ready.txt file and put in the media location
                String readyFilewithPath = hotDirectoryName + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }   
        
    private boolean logChildRecord (Integer parentVfcuMediaFileId) {
        
        VFCUMediaFile vfcuMediafile = new VFCUMediaFile();
        vfcuMediafile.setVfcuMediaFileId(parentVfcuMediaFileId);
            
        // Get the child record
        int childVfcuMediaFileId = vfcuMediafile.retrieveSubFileId();
        if (! (childVfcuMediaFileId > 0 )) {
            logger.log(Level.FINER, "Could not get child ID");
            return false;
        }
        
        vfcuMediafile.setVfcuMediaFileId(childVfcuMediaFileId);

        vfcuMediafile.populateMediaFileName();
                    
        CDISMap cdisMap = new CDISMap();

        cdisMap.setFileName(vfcuMediafile.getMediaFileName());
        cdisMap.setVfcuMediaFileId(vfcuMediafile.getVfcuMediaFileId());
        cdisMap.setCdisCisMediaTypeId(Integer.parseInt(CDIS.getProperty("cdisLinkedCisMediaTypeId")));
            
        // put the entry into the CDIS_MAP table
        boolean mapEntryCreated = cdisMap.createRecord();
            
        if (!mapEntryCreated) { 
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
            return false;
        }
             
        //Log into the activity table
        CDISActivityLog cdisActivity = new CDISActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("MIC");    
        boolean activityLogged = cdisActivity.insertActivity();
        
        if (!activityLogged) {
            logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
            return false;
        }   
        return true;
    }
    
    private void processFilesFromList () {
               
        //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair for insert into CDIS_MAP table       
        Iterator<String> it = masterMediaIds.keySet().iterator();
        
        while (it.hasNext())  {  
                
            String uniqueMediaId = it.next();
            
            //Make sure the last transaction is committed
            try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            logger.log(Level.FINEST, "Processing for uniqueMediaId: " + uniqueMediaId);
                
            CDISMap cdisMap = new CDISMap();                           
            cdisMap.setFileName(masterMediaIds.get(uniqueMediaId));
            cdisMap.setVfcuMediaFileId(Integer.parseInt(uniqueMediaId));
            
            cdisMap.populateMediaTypeId();
            
            // Now that we have the cisUniqueMediaId, Add the media to the CDIS_MAP table
            boolean mapEntryCreated = cdisMap.createRecord();
                    
            if (!mapEntryCreated) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
                    
                //Remove from the list of renditions to ingest, we dont want to bring this file over without a map entry
                it.remove();
                continue;
            }
                
            //Log into the activity table
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("MIC");    
            boolean activityLogged = cdisActivity.insertActivity();
            if (!activityLogged) {
                logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                 //Remove from the list of renditions to ingest, we dont want to bring this file over without an activity_log entry
                it.remove();
                
                //rollback the database to remove the map Entry
                try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().rollback(); } catch (Exception e) { e.printStackTrace(); }
                
                continue;
            }
            
            if (CDIS.getProperty("useMasterSubPairs").equals("true") ) {
                boolean childProcessed = logChildRecord(cdisMap.getVfcuMediaFileId());
                if (!childProcessed) {
                    it.remove();
                }
            }
        }
        
        //commit so the last record is saved for this batch so another batch doesnt pick it up
        try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }

    
    /*  Method :        populateNewMediaList
        Arguments:      
        Returns:      
        Description:    Adds to the list of media IDs that need to be integrated into DAMS
        RFeldman 3/2015
    */
    private boolean populateNewMasterMediaList () {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("idListToSend"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }   
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            String sql = xml.getSqlQuery();
            sql = sql + " AND ROWNUM < " + CDIS.getProperty("maxNumFiles") + " + 1";
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                      masterMediaIds.put(rs.getString("uniqueMediaId"), rs.getString("mediaFileName"));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
                return false;
            }
        }
        
        return true;
        
    }
    
    private boolean obtainHotFolderName() {
        //Obtain empty hot folder to put these files into
        File flHotFolderBase;
        int totalNumHotFolders;
       
        for (int hotFolderIncrement = 1;; hotFolderIncrement ++) {

            if (CDIS.getProperty("maxHotFolderIncrement").equals("0")) {
                // There is no increment, we do not append any number to the hot folder name
                hotFolderBaseName = CDIS.getProperty("hotFolderArea");
                totalNumHotFolders = 1;
                
            } else {
                // We use incremental hot folder names
                hotFolderBaseName = CDIS.getProperty("hotFolderArea") + "_" + hotFolderIncrement;
                totalNumHotFolders = Integer.parseInt(CDIS.getProperty("maxHotFolderIncrement"));
            }
            
            flHotFolderBase = new File (hotFolderBaseName);
            
            if (hotFolderIncrement >  totalNumHotFolders ) {
                
                try {
                    logger.log(Level.FINER, "Sleeping, waiting for available hot folder...");
                    Thread.sleep(300000);  //sleep 5 minutes
                    hotFolderIncrement = 0;
                    continue;
                } catch (Exception e) {
                    logger.log(Level.FINER, "Exception in Thread.sleep: " + hotFolderBaseName);
                }               
            }
            
            if (! flHotFolderBase.exists()) {
                logger.log(Level.FINER, "Unable to locate Hot Folder Main Directory: " + hotFolderBaseName);
                return false;
            }
        
            //count files in hotfolder master area
            fullMasterHotFolderNm = hotFolderBaseName + "\\MASTER";
            fullMasterHotFolder = new File(fullMasterHotFolderNm);
                
            if (! fullMasterHotFolder.exists()) {
                logger.log(Level.FINER, "Error, Could not find MASTER hotfolder directory in: ", fullMasterHotFolderNm);
                return false;
            }
            int numMasterFolderFiles = fullMasterHotFolder.list().length;
            
            //count files in hotfolder subfiles area
            String fullSubFilesHotFolderNm = hotFolderBaseName + "\\SUBFILES";
            File subFilesHotFolderNm = new File(fullSubFilesHotFolderNm);
                
            if (! subFilesHotFolderNm.exists()) {
                logger.log(Level.FINER, "Error, Could not find SUBFILES hotfolder directory in: ", subFilesHotFolderNm);
                return false;
            }
            int numSubFolderFiles = subFilesHotFolderNm.list().length;
                    
            if (numMasterFolderFiles + numSubFolderFiles == 0 ) {
                 // We found an empty hotfolder, use this one
                break;
            }
                
        }
        
        logger.log(Level.FINER, "Will use current hotfolder: " + hotFolderBaseName);
        return true;
        
    }
    
    /*  Method :        sendForingest
        Arguments:      
        Returns:      
        Description:    The main entrypoint or 'driver' for the ingestToDams operation Type
        RFeldman 3/2015
    */
     public void sendForingest () {
  
        this.masterMediaIds = new LinkedHashMap<>();
        
        // Get the list of new Media to add to DAMS
        boolean masterListGenerated = populateNewMasterMediaList ();
        if  (! masterListGenerated || masterMediaIds.isEmpty()) {
             logger.log(Level.FINER, "No Media Found to process in this batch.  Exiting");
             return;
        }
        
        // Process each media item from list (move to workfolder/hotfolder)
        logger.log(Level.FINER, "Processing media List");
        processFilesFromList();
        
        boolean hotFolderObtained = obtainHotFolderName();
        
        if (! hotFolderObtained){
             logger.log(Level.FINER, "Unable to obtain hotfolder successfully");
             return;
        }
        
        xferFilesToHotFolder ();
        
        // check to make sure if the hotfolder is there
        if ( ! fullMasterHotFolder.exists() ) {
            logger.log(Level.FINER, "Unable to find hotfolder to put ready.txt file");
            return;
        }
        
        // check to make sure there were files that were sent
        if  (! (fullMasterHotFolder.list().length > 0) ) {
            logger.log(Level.FINER, "No ready.txt file needed, no files sent to ingest folder");
        }
        
        if (CDIS.getProperty("useMasterSubPairs").equals("true") ) {
            // get the subfile location
            File subfiledir = new File (hotFolderBaseName + "\\SUBFILES") ;
                
            if  (this.masterMediaIds.size() !=  subfiledir.list().length ) {
                logger.log(Level.FINER, "Do not put ready.txt file, number of subfiles != number of master files");
                return;
            }
        }
        
        //if we have gotten this far, create the ready.txt file
        createReadyFile(fullMasterHotFolder.getName());
        
     }
     

    private boolean xferFilesToHotFolder () {
            
        //loop through the NotLinked Master Media and transfer the files
        for (String masterMediaId : masterMediaIds.keySet()) {       
            
            try {
                
                CDISMap cdisMap = new CDISMap();
                StagedFile stagedFile = new StagedFile();
            
                try { if ( CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
                VFCUMediaFile vfcuMediafile = new VFCUMediaFile();
                vfcuMediafile.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
            
                if (CDIS.getProperty("useMasterSubPairs").equals("true") ) {
                    // Get the child record
                    CDISMap cdisMapChild = new CDISMap();
                    
                    int childVfcuMediaFileId = vfcuMediafile.retrieveSubFileId();
                    if (! (childVfcuMediaFileId > 0 )) {
                        logger.log(Level.FINER, "Could not get child ID");
                        continue;
                    }
                
                    //Get the file path for the vfcu_id
                    boolean infoPopulated = stagedFile.populateNamePathFromId(childVfcuMediaFileId);
                    if (! infoPopulated) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMapChild, "CPHOTF", "Error, unable to populate name and path from database for subfile ");
                        continue;
                    }
                
                    cdisMapChild.setVfcuMediaFileId(childVfcuMediaFileId);
                    cdisMapChild.populateIdFromVfcuId();
                    cdisMapChild.setFileName(stagedFile.getFileName());
                 
                    boolean fileXferred;
                    //Find the image and move/copy to hotfolder

                    fileXferred = stagedFile.xferToHotFolder(hotFolderBaseName + "\\" + "SUBFILES", cdisMapChild.getCdisMapId()); 
                    
                      
                    if (! fileXferred) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMapChild, "XFHOTF", "Error, unable to copy file to subfile: " + stagedFile.getFileName());
                        continue;
                    }
                
                    CDISActivityLog cdisActivity = new CDISActivityLog();
                    cdisActivity.setCdisMapId(cdisMapChild.getCdisMapId());
                    cdisActivity.setCdisStatusCd("FXS");
                    boolean activityLogged = cdisActivity.insertActivity();
                    if (!activityLogged) {
                        logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                        continue;
                    }
                }
                
                //now send the master file to the hotfolder
                boolean infoPopulated = stagedFile.populateNamePathFromId(Integer.parseInt(masterMediaId));
                if (! infoPopulated) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "MVHOTF", "Error, unable to populate name and path from database for master file ");
                    continue;
                }
               
                //Get the CDIS_ID 
                cdisMap.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
                cdisMap.populateIdFromVfcuId();
               
                //Decide whether to move file or to copy it
                boolean fileMoved = stagedFile.xferToHotFolder(hotFolderBaseName + "\\" + "MASTER", cdisMap.getCdisMapId());  
                if (! fileMoved) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "MVHOTF", "Error, unable to move file to master: " + stagedFile.getFileName());
                    continue;
                }
                
                CDISActivityLog cdisActivity = new CDISActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());   
                cdisActivity.setCdisStatusCd("FMM");
                boolean activityLogged = cdisActivity.insertActivity();
                if (!activityLogged) {
                    logger.log(Level.FINER, "Could not create CDIS Activity entry, retrieving next row");
                    continue;
                }
                
            }  finally {
                
                //make sure we commit the final time through the loop
                try { if (CDIS.getDamsConn() != null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
            }
        }
        return true;
    }
}
