/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import edu.si.damsTools.cdis.cis.CisRecordAttr;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlQueryData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.CdisCisUNGroupMap;
import edu.si.damsTools.utilities.DbUtils;


/**
 * Class: LinkCisRecord
 * Purpose: This class is the main class for the linkCisRecord Operation type.
 * The linkCisRecord Operation Type links the CIS Record to CDIS (that is it adds identifiers from the CIS into the 
 * CDIS tables) The end result is a DAMS record will be associated to a CIS record in the CDIS mapping tables.
 */

public class LinkCisRecord extends Operation {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
            
    private final ArrayList<CdisMap> cdisMapList;
    
    public LinkCisRecord() {
        cdisMapList = new ArrayList();
    }
    
    // Method: invoke()
    // Purpose: the main 'driver' method for the linkCisReocrd
    public void invoke () {
    
        //Obtain a list of all the dams media to link that has never been through VFCU
        boolean idsToLink = populateCdisMapListToLink();
        if (idsToLink) {
        
            for (CdisMap cdisMap  : cdisMapList) {
                
                boolean cisRecordRecorded = false;
                
                DamsRecord damsRecord = new DamsRecord();
                damsRecord.setUoiId(cdisMap.getDamsUoiid());
                damsRecord.setBasicData();
                
                //Check if there is a matching record in the CIS for this cdisMap record
                CisRecordAttr cis = returnCorrespondingCisRecord(damsRecord);  
                if (cis == null) {
                    logger.log(Level.FINEST, "Error, unable to find matching cis");
                    continue;
                }
                
                if (cis.returnCdisGroupType().equals("un")) {
                    //use the UNGroupMap Table
                    //See if this cdis_map_id already exists in the table for any refid
                    CdisCisUNGroupMap cdisCisGroup = new CdisCisUNGroupMap();
                    cdisCisGroup.setCdisMapId(cdisMap.getCdisMapId());
                    cdisCisGroup.setCisGroupCd("ead");
                    cdisCisGroup.setCisGroupValue(damsRecord.getSiAssetMetadata().getEadRefId());
                    cdisCisGroup.populateIdForMapIDGroupCdCis();
                    if (cdisCisGroup.getCdisCisGroupMapId() != null) {
                        //We can find the groupID for the cdis_map_id.
                        //update the group_value
                        cisRecordRecorded = cdisCisGroup.updateCisGroupValue();
                        if (!cisRecordRecorded) {
                            ErrorLog errorLog = new ErrorLog ();
                            errorLog.capture(cdisMap, "UPCCIS", "Error, unable to create CIS record");
                            continue;
                        }                 
                    }
                    else {
                        cisRecordRecorded = cdisCisGroup.createRecord();
                        if (!cisRecordRecorded) {
                            ErrorLog errorLog = new ErrorLog ();
                            errorLog.capture(cdisMap, "UPCCIS", "Error, unable to create CIS record");
                            continue;
                        }
                    }    
                }
                else {
                    //support the old stuff as well
                    cdisMap.setCisUniqueMediaId(cis.getCisImageIdentifier());
                    cisRecordRecorded = cdisMap.updateCisUniqueMediaId(); 
             
                    if (!cisRecordRecorded) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMap, "UPCCIS", "Error, unable to create CIS record");
                        continue;
                    }
                }

                ///need to take care of emu here....there is no object level information
                if (cis.returnCdisGroupType() != null && cis.returnCdisGroupType().equals("cdisObjectMap")) {
                    CdisObjectMap cdisObjectMap = new CdisObjectMap();
                    cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
                    cdisObjectMap.setCisUniqueObjectId(cis.getGroupIdentifier());
                    boolean cdisObjectCreated = cdisObjectMap.createRecord();
                    if (! cdisObjectCreated) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMap, "UPCCIS", "Error, unable to create CDIS object record");
                        continue;
                    }
                }
                
                //update the thumbnail if needed.  This should probably belong in CIS Update tool
                if ( ! (DamsTools.getProperty("updateTMSThumbnail") == null) && DamsTools.getProperty("updateTMSThumbnail").equals("true") ) {
                    boolean thumbnailUpdated = updateCisThumbnail(cdisMap.getCdisMapId());
                    if (!thumbnailUpdated) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMap, "CRCIST", "Error, unable to create CIS thumbnail");
                        continue;
                    }
                }
            
                //Add the status
                CdisActivityLog cdisActivity = new CdisActivityLog();
                cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
                cdisActivity.setCdisStatusCd("LCC");
        
                boolean statusLogged = cdisActivity.updateOrInsertActivityLog();
                
                if (!statusLogged) {
                     ErrorLog errorLog = new ErrorLog ();
                     errorLog.capture(cdisMap, "UPCCIS", "Error, unable to update status for record");
                }
                
                
                try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
                try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            }   
        }  
    }
    
    // Method: populateCdisMapListToLink()
    // Purpose: Populates the list of CdisMap records that require linking using the criteria in the xml file
    private boolean populateCdisMapListToLink() {
        
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
                cdisMap.populateMapInfo();
                cdisMapList.add(cdisMap);
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain list of Map IDs to integrate", e);
            return false;
        }
        return true;
    }
    
    // Method: returnCorrespondingCisRecord()
    // Purpose: Returns the associated Cis record that corresponds to the damsRecord passed to it.
    //          The rules that associate the damsRecord to the CIS record are found in the xml config file
    private CisRecordAttr returnCorrespondingCisRecord(DamsRecord damsRecord) {
        
        CisRecordAttr cis = null;
        String dbConnectionStr = null;
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","retrieveCisIds");         
            dbConnectionStr = xmlInfo.getAttributeData("dbConn");
            
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "retrieveCisId sql not found");
            return null;
        }
        
        sql = damsRecord.replaceSqlVars(sql);
               
        logger.log(Level.FINEST, "SQL: {0}", sql);

        Connection dbConn = DbUtils.returnDbConnFromString(dbConnectionStr);
        if (dbConn == null) {
            logger.log(Level.FINEST, "Error: connection to db not found");
        }
        
        try (PreparedStatement stmt = dbConn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            if (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();            

                CisRecordFactory cisFact = new CisRecordFactory();
                cis = cisFact.cisChooser();
                cis.setBasicValues(rs.getString(1), damsRecord);        
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain corresponding CIS Records", e);
            return null;
        }
        return cis;
    }
    
    // Method: updateCisThumbnail()
    // Purpose: updates the thumbnail in the CIS based on the image that resides in the DAMS repository
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
    
    // Method: returnRequiredProps()
    // Purpose: We set the required property list to validate the .ini file
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("linkCisRecordXmlFile");
        reqProps.add("cis");
        
        //add more required props here
        return reqProps;    
    }
}
