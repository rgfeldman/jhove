/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import edu.si.damsTools.cdis.cis.CisRecordAttr;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class LinkCisRecord extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
            
    private final ArrayList<CdisMap> cdisMapList;
    
    public LinkCisRecord() {
        cdisMapList = new ArrayList();
    }
    
    
    public void invoke () {
        //Obtain a list of all the dams media to link that has never been through VFCU
        boolean idsToLink = populateCdisMapIdsToLink();
        if (idsToLink) {
        
            for (CdisMap cdisMap  : cdisMapList) {
                //Check if there is a matching record in the CIS for this cdisMap record
                //getCisIdentifier
                
                //Add the CIS unique Media ID
            
                //Add the object_map record
                
                //update the thumbnail if needed
                if ( ! (DamsTools.getProperty("updateTMSThumbnail") == null) && DamsTools.getProperty("updateTMSThumbnail").equals("true") ) {
                    updateCisThumbnail(cdisMap.getCdisMapId());
                }
            
                //Add the status
            
            }   
        }  
    }
    
    private boolean populateCdisMapIdsToLink() {
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveMapIds");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "retrieveMapIds sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) {
                CdisMap cdisMap = new CdisMap();
                cdisMap.setCdisMapId(rs.getInt(1));
                cdisMapList.add(cdisMap);
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain list of Map IDs to integrate", e);
            return false;
        }
        return true;
    }
    
    
    private CisRecordAttr returnCorrespondingCisRecord() {
        
        CisRecordAttr cis = null;
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveCisRecord");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "retrieveCisRecord sql not found");
            return null;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);

        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            
            //Add the value from the database to the list
            if (rs.next()) {
                CisRecordFactory cisFact = new CisRecordFactory();
                cis = cisFact.cisChooser();
                
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain list of Map IDs to integrate", e);
            return null;
        }
        return cis;
    }
    
    private boolean updateCisThumbnail(int cdisMapId) {

        Thumbnail thumbnail = new Thumbnail();
        boolean thumbCreated = thumbnail.generate(cdisMapId);
                           
        if (! thumbCreated) {
            logger.log(Level.FINER, "CISThumbnailSync creation failed");
            return false;
        }
            
        CdisActivityLog cdisActivity = new CdisActivityLog();
        cdisActivity.setCdisMapId(cdisMapId);
        cdisActivity.setCdisStatusCd("CTS");
        cdisActivity.insertActivity();
           
        return true;
    }
    
    
    
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        return reqProps;    
    }
}
