/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.cdis.cis.CisRecordAttr;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.utilities.XmlQueryData;
import edu.si.damsTools.cdisutilities.ErrorLog;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


/*
 * Class: CisUpdate
 * Purpose: This class is the main class for the cisUpdate Operation type.
 * The purpose of this operation type is to update values in the CIS based on values found in DAMS.
 * The values to be updated typically require links being established with the linkCisRecord and linkDamsRecord operations
 */

public class CisUpdate extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<DamsRecord> damsRecordList;
            
    public CisUpdate() {
        damsRecordList = new ArrayList<>();
    }
    
    /* Member Funtion: generateCisSql
     * Purpose: obtains the SQL to perform the update on the CIS sourced from the XML file
     */
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
        

        return (sql);
    }
    
    /* Member Function: invoke
     * Purpose: the main driver routine for this operation 
     */
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
     
    /* Member Function: populateDamsRecordList
     * Purpose: obtains the SQL that contains the selection criteria from DAMS, and populates the DamsRecord list with the return values
     */
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
            logger.log(Level.SEVERE, "Error obtaining list to update in CIS", e);
            return false;
        }
        return true;
    }
    
    /* Member Function: processRecordList
     * Purpose: Steps through the list of records that must be updated, and delegates work to be done...one record at a time.
     */
    private void processRecordList () {
        
        CisRecordAttr cis;
        CisRecordFactory cisFact = new CisRecordFactory();
            
        for (DamsRecord damsRecord : this.damsRecordList) {
        
            CdisMap cdisMap = new CdisMap();
            cdisMap.setDamsUoiid(damsRecord.getUois().getUoiid());
            cdisMap.populateIdFromUoiid();
            cdisMap.populateCisUniqueMediaIdForUoiid();
            
            cis = cisFact.cisChooser();
            cis.setBasicValues(cdisMap.getCisUniqueMediaId(), damsRecord.getUois().getUoiid());
            
            //populate the Cis
            String cisSql = generateCisSql(cis);
            if (cisSql == null) {
                //unable to generate SQL, generate error
                continue;
            }
            cisSql = damsRecord.replaceSqlVars(cisSql);
            if (cisSql.contains("?MEDIA_ID?")) {
                cisSql = cisSql.replace("?MEDIA_ID?", cis.getCisImageIdentifier());
                logger.log(Level.FINEST, "New SQL: "+ cisSql);
            }
            
            int numRowsUpdate = updateCis(cisSql);
            if (! (numRowsUpdate > 0)) {
                //unable to generate SQL, generate error
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPCISI", "Error, update of CIS info failed");
                continue;
            } 
            
            //IF successful, populate ead_ref_id_log
            boolean addtlUpdatesDone = cis.additionalCisUpdateActivity(damsRecord, cdisMap);
            if (! addtlUpdatesDone) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPCISI", "Error, update of additional CIS info failed");
                continue;
            }
            
            //Populate Activity Log
            //Insert row in the activity_log as completed. COMMENTED OUT FOR NOW
            CdisActivityLog cdisActivity = new CdisActivityLog(); 
            cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
            cdisActivity.setCdisStatusCd(cis.returnCisUpdateCode()); 
            boolean activityLogged = cdisActivity.insertActivity();
            if (!activityLogged) {
                logger.log(Level.FINER, "Error, unable to create CDIS activity record ");
            }
            
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        }
    }    
    
    /* Member Function: returnRequiredProps
     * Purpose: Returns a list of required properties that must be set in the properties file in otder to run the cisUpate operation
     */ 
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        reqProps.add("cis");
        reqProps.add("cisDriver");
        reqProps.add("cisConnString");
        reqProps.add("cisUser");
        reqProps.add("cisPass");
        reqProps.add("cisUpdateXmlFile");
        
        //add more required props here
        return reqProps;    
    }
    
    /* Member Function: updateCis
     * Purpose: Performs the actual update of the CIS
     */
    private int updateCis (String sql) {
        
        int recordsUpdated = 0;
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql)) {
 
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in Cis " + recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINEST,"Error updating CIS data", e);    
        } 
        return recordsUpdated;
    }
}
