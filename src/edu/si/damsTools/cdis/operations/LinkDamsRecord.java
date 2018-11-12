/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

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
import edu.si.damsTools.cdisutilities.ErrorLog;


/**
 * Class: LinkDamsRecord
 * Purpose: This class is the main class for the linkDamsRecord Operation type.
 * The linkDamsRecord Operation Type links the DAMS record to CDIS (that is it adds the UOI_ID identifier from DAMS into the 
 * CDIS tables). The end result is a DAMS record will be associated to a CDIS record.
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
                
                try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            }
        }
        
         //This next block is for media to link that has been through VFCU
        boolean cdisListPopulated = populateCdisMediaList();
        if (cdisListPopulated) {
            for (CdisMap cdisMap : this.cdisMapList) {
                boolean recordLinked = linkRecordFromVfcu(cdisMap);  
                if(! recordLinked) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPCDAM", "Error, update of CDIS with DAMS info failed");
                    continue;
                }
                logActivity(cdisMap);
                
                try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            } 
        }    
    }
    
    
    private boolean linkRecordFromVfcu(CdisMap cdisMap) {
        cdisMap.updateUoiid();
                
        DamsRecord damsRecord = new DamsRecord();
        damsRecord.setUoiId(cdisMap.getDamsUoiid());
        damsRecord.setBasicData();
                
        //Add the preservation information
        boolean preservationAdded = damsRecord.addPreservationData(cdisMap);
        if (!preservationAdded) {
            ErrorLog errorLog = new ErrorLog ();
            errorLog.capture(cdisMap, "UPDAMP", "Error, unable to insert preservation data");
            return false;
        }
                   
        //Move file to the emu pickup area if necessary
        if (! DamsTools.getProperty("retainAfterIngest").equals("false") &&
            cdisMap.getFileName().endsWith(DamsTools.getProperty("retainAfterIngest")) ) {
            
            boolean fileMoved = postIngestMove(cdisMap);  
            if (! fileMoved) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "CPDELP", "Error, unable to move file to pickup location");
                return false;
            }
        }    
        
        return true;
    }
    
    private boolean postIngestMove(CdisMap cdisMap) {
        
        if (DamsTools.getProperty("postIngestDeliveryLoc") == null) {
            logger.log(Level.FINEST, "Error, Post ingest delivery site never specified");
            return false;
        }
  
        StagedFile stagedFile = new StagedFile();
        stagedFile.populateNameStagingPathFromId(cdisMap.getVfcuMediaFileId());
        boolean fileDelivered = stagedFile.deliverForPickup(DamsTools.getProperty("postIngestDeliveryLoc"));
 
        if (!fileDelivered) {
            return false;
        }
        CdisActivityLog activityLog = new CdisActivityLog();
        activityLog.setCdisMapId(cdisMap.getCdisMapId());
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
        
        boolean mediaCreated = cdisMap.createRecord();
        
        if (! mediaCreated) {
            return null;
        }
        
        return cdisMap;
    }
    
    private boolean populateDamsMediaList () {
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveDamsIds");
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
                cdisMap.populateVfcuMediaFileId();
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
        reqProps.add("retainAfterIngest");
        reqProps.add("linkDamsRecordXmlFile");
        
        if (! DamsTools.getProperty("retainAfterIngest").equals("false") ) {
            reqProps.add("postIngestDeliveryLoc");
        }
        //add more required props here
        return reqProps;    
    }
     
}
