/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MarkDamsDelete extends Operation{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
     
    private final ArrayList<DamsRecord> damsRecordList;
    
    public MarkDamsDelete() {
        this.damsRecordList = new ArrayList();
    }

        
    public void invoke() {
         boolean deletionListPopulated = populateDamsMediaList();         
         if (! deletionListPopulated) {
             logger.log(Level.FINEST, "unable to populate list of dams records to delete");
             return;
         }
         
         for (DamsRecord damsRecord: damsRecordList) {
            markDamsRecordDelete(damsRecord);
             
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
         }
         
    }
    
    private void markDamsRecordDelete(DamsRecord damsRecord) {
        
        CdisMap cdisMap = new CdisMap();
        cdisMap.setDamsUoiid(damsRecord.getUois().getUoiid());
        cdisMap.populateIdFromUoiid();
        
        int numRowsDeleted = damsRecord.getUois().markDelete();
        
        if (numRowsDeleted != 1) {
             ErrorLog errorLog = new ErrorLog ();
             errorLog.capture(cdisMap, "DELDAM", "Error, deletion from DAMS failed");
             return;
        }
        
        //mark cdis status as row deleted
        CdisActivityLog cdisActivity = new CdisActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("MDD");
        cdisActivity.insertActivity();  
                
    }
    
    private boolean populateDamsMediaList () {
        String sql = XmlUtils.returnFirstSqlForTag("retrieveDamsIds");    
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
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("sqlFile");
        
        return reqProps;
    }
        
}
