/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.DamsTools;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.dams.StagedFile;
import edu.si.damsTools.cdis.dams.database.TeamsLinks;
import edu.si.damsTools.cdis.database.MediaTypeConfigR;
import edu.si.damsTools.cdisutilities.ErrorLog;


/**
 *
 * @author rfeldman
 */
public class LinkDamsRecord extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<DamsRecord> damsRecordList;
    private final ArrayList<CdisMap> cdisMapList;
    
    public LinkDamsRecord () {
        damsRecordList = new ArrayList();
        cdisMapList = new ArrayList();
    }
    
    public void invoke () {
        
        //Obtain a list of all the dams media to link that has never been through VFCU
        boolean unlinkedListPopulated = populateDamsMediaList();
        if (unlinkedListPopulated) {
            //These are records that we need to add to CDIS table
            //Add the records and add to the cdis_map_list
            for (DamsRecord damsRecord : damsRecordList) {    
                CdisMap cdisMap = new CdisMap();
                cdisMap = createNewRecordToLink(damsRecord);
                if (cdisMap == null) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "CRCDMP", "Error, unable to create CDIS record");
                    continue;
                }
                logActivity(cdisMap); 
                
                try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            }
        }
        
         //This next block is for media to link that has been through VFCU
        boolean cdisListPopulated = populateCdisMediaList();
        if (cdisListPopulated) {
            for (CdisMap cdisMap : this.cdisMapList) {
                boolean recordLinked = linkRecordFromVfcu(cdisMap);  
                if(! recordLinked) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPCCIS", "Error, update of CIS info in CDIS failed");
                    continue;
                }
                logActivity(cdisMap);
                
                try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            } 
        }    
    }
    
    
    private boolean linkRecordFromVfcu(CdisMap cdisMap) {
        cdisMap.updateUoiid();
                
        DamsRecord damsRecord = new DamsRecord();
        damsRecord.setUoiId(cdisMap.getDamsUoiid());
        damsRecord.setBasicData();
                
        //THE NEXT SHOULD BE DONE TO THE DAMS RECORD, not HERE
        //link parent and child records if necessary
        if ( DamsTools.getProperty("linkHierarchyInDams").equals("true") ) {
            establishParentChildLink(cdisMap);
        }
                
        //Add the preservation information
        boolean preservationAdded = damsRecord.addPreservationData(cdisMap);
        if (!preservationAdded) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMP", "Error, unable to insert preservation data");
            return false;
        }
                   
        //Move file to the emu pickup area if necessary
        if ( DamsTools.getProperty("retainFilesPostIngest").equals("true") ) {
                    
            boolean fileMoved = postIngestMove(cdisMap.getVfcuMediaFileId());  
            if (! fileMoved) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CPDELP", "Error, unable to move file to pickup location");
                 return false;
            }
        }   
        
        return true;
    }
    
    private boolean postIngestMove(int vfcuMediaFileId) {
        
        if (DamsTools.getProperty("postIngestDeliveryLoc") == null) {
            logger.log(Level.FINEST, "Error, Post ingest delivery site never specified");
            return false;
        }
  
        StagedFile stagedFile = new StagedFile();
        stagedFile.populateNameStagingPathFromId(vfcuMediaFileId);
        boolean fileDelivered = stagedFile.deliverForPickup(DamsTools.getProperty("postIngestDeliveryLoc"));
 
        if (!fileDelivered) {
            return false;
        }
        CdisActivityLog activityLog = new CdisActivityLog();
        activityLog.setCdisStatusCd("FME");
        activityLog.insertActivity();
        
        return true;
    }

    
    private boolean logActivity(CdisMap cdisMap) {
        CdisActivityLog cdisActivity = new CdisActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("LDC");
        cdisActivity.insertActivity();
                        
        return true;
    }
        
    private CdisMap createNewRecordToLink(DamsRecord damsRecord) {
        
        CdisMap cdisMap = new CdisMap();
        
        cdisMap.setDamsUoiid(damsRecord.getUois().getUoiid());
        cdisMap.setFileName(damsRecord.getUois().getName());
        cdisMap.populateMediaTypeId();
        
        boolean mediaCreated = cdisMap.createRecord();
        
        if (! mediaCreated) {
            return null;
        }
        
        return cdisMap;
    }
    
    private boolean populateDamsMediaList () {
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "retrieveDamsIds sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) {
                DamsRecord damsRecord = new DamsRecord();
                damsRecord.setUoiId(rs.getString(1));
                damsRecord.setBasicData();
                damsRecordList.add(damsRecord);
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain list of UOI_IDs to integrate", e);
            return false;
        }
        return true;
    }
    
    private boolean populateCdisMediaList () {
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveVfcuIngestedIds");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "retrieveVfcuIngestedIds sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) {
                CdisMap cdisMap = new CdisMap();
                cdisMap.setCdisMapId(rs.getInt(1));
                cdisMap.setDamsUoiid(rs.getString(2));
                cdisMap.populateCdisCisMediaTypeId();
                cdisMapList.add(cdisMap);
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error, unable to obtain MapId list to integrate", e);
            return false;
        }
        return true;     
    }
    
     public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("linkHierarchyInDams");
        reqProps.add("retainFilesPostIngest");
        reqProps.add("linkDamsRecordXmlFile");
        //add more required props here
        return reqProps;    
    }
     
     //THIS NEEDS TO BE RE-WRITTEDN IN OO 
    public boolean establishParentChildLink (CdisMap cdisMap) {
        
        //populate the Parent ID from the db
        MediaTypeConfigR mediaTypeConfigR = new MediaTypeConfigR();
        mediaTypeConfigR.setMediaTypeConfigId(cdisMap.getMediaTypeConfigId());
        
        //populate the parent and child ID from the db
        mediaTypeConfigR.populateChildAndParentOfId();
          
        CdisMap childCdisMap = new CdisMap();
        
        if (mediaTypeConfigR.getChildOfId() > 0 ) {
            
            CdisMap parentCdisMap = new CdisMap();
            
            boolean parentInfoPopulated = parentCdisMap.populateParentFileInfo(cdisMap.getCdisMapId() );
            if (parentInfoPopulated) {
                TeamsLinks teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
                teamsLinks.setDestValue(parentCdisMap.getDamsUoiid());
                teamsLinks.setLinkType("CHILD");
                teamsLinks.createRecord();
        
                teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(parentCdisMap.getDamsUoiid());
                teamsLinks.setDestValue(cdisMap.getDamsUoiid());
                teamsLinks.setLinkType("PARENT");
                teamsLinks.createRecord();
            }
            else {
                logger.log(Level.FINER, "unable to obtain parent info ");
            }
        }
        
        if (mediaTypeConfigR.getParentOfId() > 0 ) {
            boolean childInfoPopulated = childCdisMap.populateChldFileInfo(cdisMap.getCdisMapId() );
            
            if (childInfoPopulated) {
                TeamsLinks teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(childCdisMap.getDamsUoiid());
                teamsLinks.setDestValue(cdisMap.getDamsUoiid());
                teamsLinks.setLinkType("CHILD");
                teamsLinks.createRecord();
        
                teamsLinks = new TeamsLinks();
                teamsLinks.setSrcValue(cdisMap.getDamsUoiid());
                teamsLinks.setDestValue(childCdisMap.getDamsUoiid());
                teamsLinks.setLinkType("PARENT");
                teamsLinks.createRecord();
            }
            else {
                logger.log(Level.FINER, "unable to obtain child info ");
            }       
        }
        
        return true;
    }
}
