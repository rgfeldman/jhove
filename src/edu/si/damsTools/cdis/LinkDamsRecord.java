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
import edu.si.damsTools.cdis.database.CDISActivityLog;
import edu.si.damsTools.cdis.dams.database.SiPreservationMetadata;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.dams.MediaRecord;


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
            
                if (cdisMap != null) {
                    cdisMapList.add(cdisMap);
                } 
            }
        }
        
         //Obtain a list of all the dams media to link that has been through VFCU
        boolean cdisListPopulated = populateCdisMediaList();
        if (cdisListPopulated) {
            for (CdisMap cdisMap : this.cdisMapList) {
                cdisMap.updateUoiid();
                
                VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
                vfcuMediaFile.setVfcuMediaFileId(cdisMap.getVfcuMediaFileId());
                vfcuMediaFile.populateVendorChecksum();
                
                SiPreservationMetadata prev = new SiPreservationMetadata();
                prev.setPreservationIdNumber(vfcuMediaFile.getVendorChecksum());
                prev.insertRow();
                
            } 
        }    
        
        
        //Now for both cdisMaps that were added OR cdisMaps ingested from VFCU, we need to do other things
        
        for (CdisMap cdisMap : cdisMapList) {
            
            //update the thumbnail if needed
            if ( ! (DamsTools.getProperty("updateTMSThumbnail") == null) && DamsTools.getProperty("updateTMSThumbnail").equals("true") ) {
                updateCisThumbnail(cdisMap.getCdisMapId());
            }
            
            //link parent and child records if necessary
            if ( (DamsTools.getProperty("linkHierarchyInDams") != null ) && (DamsTools.getProperty("linkHierarchyInDams").equals("true") ) ) {
                MediaRecord mediaRecord = new MediaRecord();
                mediaRecord.establishParentChildLink(cdisMap);
            }
                        
            logActivity(cdisMap);    

        }

    }


    private boolean updateCisThumbnail(int cdisMapId) {

            Thumbnail thumbnail = new Thumbnail();
            boolean thumbCreated = thumbnail.generate(cdisMapId);
                            
            if (! thumbCreated) {
                logger.log(Level.FINER, "CISThumbnailSync creation failed");
                return false;
            }
            
            CDISActivityLog cdisActivity = new CDISActivityLog();
            cdisActivity.setCdisMapId(cdisMapId);
            cdisActivity.setCdisStatusCd("CTS");
            cdisActivity.insertActivity();
            
        return true;
    }
        
    
    
    private boolean logActivity(CdisMap cdisMap) {
        CDISActivityLog cdisActivity = new CDISActivityLog();
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
        //add more required props here
        return reqProps;    
    }
    
    
}
