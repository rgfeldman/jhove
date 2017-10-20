/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.cis.CisRecordAttr;;
import edu.si.damsTools.cdis.dams.DamsRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import edu.si.damsTools.utilities.XmlQueryData;

/**
 *
 * @author rfeldman
 */

public class CisUpdate extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<DamsRecord> damsRecordList;
            
    public CisUpdate() {
        damsRecordList = new ArrayList<>();
    }
    
    private boolean populateDamsRecordList() {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveDamsIds");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
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
            logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
            return false;
        }
        return true;
    }
    
    private int updateCis (String sql) {
        
        int recordsUpdated = 0;
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql)) {
 
            recordsUpdated = pStmt.executeUpdate(sql);
            
            logger.log(Level.FINEST,"Rows Updated in Cis! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINEST,"Error updating DAMS data", e);    
        } 
        return recordsUpdated;
   
    }
    
    
    private String generateCisSql(CisRecordAttr cis) {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","updateCis");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "Cis Update sql not found");
            return null;
        }
        
        if (sql.contains("?MEDIA_ID?")) {
            sql = sql.replace("?MEDIA_ID?", cis.getCisImageIdentifier());
        }
        
        return (sql);

    }
    
    private void processRecordList () {
        
        CisRecordAttr cis;
        CisRecordFactory cisFact = new CisRecordFactory();
            
        for (DamsRecord damsRecord : this.damsRecordList) {
        
            CdisMap cdisMap = new CdisMap();
            cdisMap.setDamsUoiid(damsRecord.getUois().getUoiid());
            cdisMap.populateIdFromUoiid();
            
            cis = cisFact.cisChooser();
            cis.setBasicValues(damsRecord.getUois().getUoiid());
            
            //populate the Cis
            String cisSql = generateCisSql(cis);
            if (cisSql == null) {
                //unable to generate SQL, generate error
                continue;
            }
            
            int numRowsUpdate = updateCis(cisSql);
            if (! (numRowsUpdate > 0)) {
                //unable to generate SQL, generate error
                continue;
            } 
            
            //IF successful, populate ead_ref_id_log
            cis.additionalCisUpdateActivity(damsRecord);
            
            //Populate Activity Log
            //Insert row in the activity_log as completed. COMMENTED OUT FOR NOW
            CdisActivityLog cdisActivity = new CdisActivityLog(); 
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd("CPD"); 
            boolean activityLogged = cdisActivity.insertActivity();
            if (!activityLogged) {
                logger.log(Level.FINER, "Error, unable to create CDIS activity record ");
            }
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        }
    }    
    
    public void invoke() {
        
        boolean receivedList = populateDamsRecordList();
        if (! receivedList) {
             logger.log(Level.FINER, "Error retrieving list of records to sync, returning ");
             return;
        }
        if (damsRecordList == null) {
            logger.log(Level.FINER, "Nothing detected to sync, returning");
            return;
        }
        
        processRecordList();
 
        try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        reqProps.add("cis");
        reqProps.add("cisDriver");
        reqProps.add("cisConnString");
        reqProps.add("cisUser");
        reqProps.add("cisPass");
        
        //add more required props here
        return reqProps;    
    }
        
}
