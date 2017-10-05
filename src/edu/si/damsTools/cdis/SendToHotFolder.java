/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.dams.StagedFile;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.cdisutilities.ErrorLog;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlQueryData;

/**
 *
 * @author rfeldman
 */
public class SendToHotFolder extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    LinkedHashMap <String,String> masterMediaIds; 
    
    private String hotFolderBaseName;
    private String fullMasterHotFolderNm;
    private File fullMasterHotFolder;
            
    public SendToHotFolder() {

    }
    
    
    private void createReadyFile () {
        
        try {
                //Create the ready.txt file and put in the media location
                String readyFilewithPath = fullMasterHotFolderNm + "/ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
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
        cdisMap.populateMediaTypeId();
        
        // put the entry into the CDIS_MAP table
        boolean mapEntryCreated = cdisMap.createRecord();
            
        if (!mapEntryCreated) { 
            //Set the mapid to null because the mapId failed upon insert
            cdisMap.setCdisMapId(null);
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
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
            try { if ( DamsTools.getDamsConn()!= null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            
            logger.log(Level.FINEST, "Processing for uniqueMediaId: " + uniqueMediaId);
                
            CdisMap cdisMap = new CdisMap();                           
            cdisMap.setFileName(masterMediaIds.get(uniqueMediaId));
            cdisMap.setVfcuMediaFileId(Integer.parseInt(uniqueMediaId));
            
            cdisMap.populateMediaTypeId();
            
            // Now that we have the cisUniqueMediaId, Add the media to the CDIS_MAP table
            boolean mapEntryCreated = cdisMap.createRecord();
                    
            if (!mapEntryCreated) {
                ErrorLog errorLog = new ErrorLog ();
                //Set the mapid to null because the mapId failed upon insert
                cdisMap.setCdisMapId(null);
                errorLog.capture(cdisMap, "CRCDMP", "Could not create CDISMAP entry, retrieving next row");
                    
                //Remove from the list of renditions to ingest, we dont want to bring this file over without a map entry
                it.remove();
                continue;
            }
            
            if (DamsTools.getProperty("useMasterSubPairs").equals("true") ) {
                boolean childProcessed = logChildRecord(cdisMap.getVfcuMediaFileId());
                if (!childProcessed) {
                    it.remove();
                }
            }
        }
        
        //commit so the last record is saved for this batch so another batch doesnt pick it up
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }

    
    /*  Method :        populateNewMediaList
        Arguments:      
        Returns:      
        Description:    Adds to the list of media IDs that need to be integrated into DAMS
        RFeldman 3/2015
    */
    private boolean populateNewMasterMediaList () {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","idListToSend");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        sql = sql + " AND ROWNUM < " + DamsTools.getProperty("maxNumFiles") + " + 1";
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) 
                masterMediaIds.put(rs.getString("uniqueMediaId"), rs.getString("mediaFileName"));
            }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
            return false;
        }
        
        return true;
        
    }
    
    private boolean obtainHotFolderName() {
        //Obtain empty hot folder to put these files into
        File flHotFolderBase;
        int totalNumHotFolders;
       
        for (int hotFolderIncrement = 1;; hotFolderIncrement ++) {

            if (DamsTools.getProperty("maxHotFolderIncrement").equals("0")) {
                // There is no increment, we do not append any number to the hot folder name
                hotFolderBaseName = DamsTools.getProperty("hotFolderArea");
                totalNumHotFolders = 1;
                
            } else {
                // We use incremental hot folder names
                hotFolderBaseName = DamsTools.getProperty("hotFolderArea") + "_" + hotFolderIncrement;
                totalNumHotFolders = Integer.parseInt(DamsTools.getProperty("maxHotFolderIncrement"));
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
            fullMasterHotFolderNm = hotFolderBaseName + "/MASTER";
            fullMasterHotFolder = new File(fullMasterHotFolderNm);
                
            if (! fullMasterHotFolder.exists()) {
                logger.log(Level.FINER, "Error, Could not find MASTER hotfolder directory in: ", fullMasterHotFolderNm);
                return false;
            }
            int numMasterFolderFiles = fullMasterHotFolder.list().length;
            
            //count files in hotfolder subfiles area
            String fullSubFilesHotFolderNm = hotFolderBaseName + "/SUBFILES";
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
     public void invoke () {

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
            return;
        }
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true") ) {
            // get the subfile location
            File subfiledir = new File (hotFolderBaseName + "/SUBFILES") ;
                
            if  (this.fullMasterHotFolder.list().length !=  subfiledir.list().length ) {
                logger.log(Level.FINER, "Do not put ready.txt file, number of subfiles != number of master files");
                logger.log(Level.FINER, "Number of Masterfiles: " + this.fullMasterHotFolder.list().length);
                logger.log(Level.FINER, "Number of Subfiles: " + subfiledir.list().length);
                return;
            }      
        }
        
        //if we have gotten this far, create the ready.txt file
       createReadyFile();
        
     }
     

    private boolean xferFilesToHotFolder () {
            
        //loop through the NotLinked Master Media and transfer the files
        for (String masterMediaId : masterMediaIds.keySet()) {       
            
            try {
                
                CdisMap cdisMap = new CdisMap();
                StagedFile stagedFile = new StagedFile();
            
                try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                
                VfcuMediaFile vfcuMediafile = new VfcuMediaFile();
                vfcuMediafile.setVfcuMediaFileId(Integer.parseInt(masterMediaId));
            
                if (DamsTools.getProperty("useMasterSubPairs").equals("true") ) {
                    // Get the child record
                    CdisMap cdisMapChild = new CdisMap();
                    
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

                    fileXferred = stagedFile.xferToHotFolder(hotFolderBaseName + "/" + "SUBFILES", cdisMapChild.getCdisMapId()); 
                    
                      
                    if (! fileXferred) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMapChild, "XFHOTF", "Error, unable to copy file to subfile: " + stagedFile.getFileName());
                        continue;
                    }
                
                    CdisActivityLog cdisActivity = new CdisActivityLog();
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
                boolean fileMoved = stagedFile.xferToHotFolder(hotFolderBaseName + "/" + "MASTER", cdisMap.getCdisMapId());  
                if (! fileMoved) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "MVHOTF", "Error, unable to move file to master: " + stagedFile.getFileName());
                    continue;
                }
                
                CdisActivityLog cdisActivity = new CdisActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());   
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
        reqProps.add("hotFolderArea");
        reqProps.add("maxNumFiles");
        reqProps.add("maxHotFolderIncrement");
        reqProps.add("useMasterSubPairs");
        return reqProps;    
    }

}
