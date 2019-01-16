/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.identifier.IdentifierFactory;
import edu.si.damsTools.cdis.cis.identifier.IdentifierType;
import edu.si.damsTools.cdis.cis.tms.Thumbnail;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdisutilities.ErrorLog;
import edu.si.damsTools.utilities.XmlData;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.utilities.DbUtils;
import edu.si.damsTools.utilities.XmlUtils;


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
        boolean MapRecordsToLink = populateCdisMapListToLink();
        if (! MapRecordsToLink) {
            return;
        }
        
        for (CdisMap cdisMap  : cdisMapList) {
                
            boolean cisRecordRecorded = false;
                
            DamsRecord damsRecord = new DamsRecord();
            damsRecord.setUoiId(cdisMap.getDamsUoiid());
            damsRecord.setBasicData();
                
            IdentifierType identType = returnCorrespondingCisRecord(damsRecord);
                
            if (identType == null) {
                logger.log(Level.FINEST, "Assoicated record cannot be found in CIS");
                continue;
            }
                    
            //See if this cdis_map_id already exists in the table for any refid
            CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
            cdisCisIdentifierMap.setCdisMapId(cdisMap.getCdisMapId());
            cdisCisIdentifierMap.setCisIdentifierCd(identType.getIdentifierCd());        
            cdisCisIdentifierMap.setCisIdentifierValue(identType.getIdentifierValue());
                   
            if (identType.overwriteExistingLinkId()) {
                
                //for ead we can update the group_value to the new one if we find it
                cdisCisIdentifierMap.populateIdForMapIDIdentifierCdCis();
                if (cdisCisIdentifierMap.getCdisCisIdentifierMapId() != null) {
                    //We found an existing groupID for the cdis_map_id. We overwite existing link rather than create a new one
                    cisRecordRecorded = cdisCisIdentifierMap.updateCisIdentifierValue();
                    if (!cisRecordRecorded) {
                        ErrorLog errorLog = new ErrorLog ();
                        errorLog.capture(cdisMap, "UPCCIS", "Error, unable to create CIS record");
                        continue;
                    }  
                }
            } 
            
            if (cdisCisIdentifierMap.getCdisCisIdentifierMapId() == null) {
                cisRecordRecorded = cdisCisIdentifierMap.createRecord();
                if (!cisRecordRecorded) {
                    ErrorLog errorLog = new ErrorLog ();
                    errorLog.capture(cdisMap, "UPCCIS", "Error, unable to create CIS record");
                    continue;
                }       
            }

            //update the thumbnail if needed.  This should probably belong in CIS Update tool
            if ( ! (DamsTools.getProperty("updateTmsThumbnail") == null) && DamsTools.getProperty("updateTmsThumbnail").equals("true") ) {
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
            cdisActivity.setCdisStatusCd("LCC-" + DamsTools.getProperty("lccIdType").toUpperCase());
        
            boolean statusLogged = cdisActivity.updateOrInsertActivityLog();
               
            if (!statusLogged) {
                ErrorLog errorLog = new ErrorLog ();
                errorLog.capture(cdisMap, "UPCCIS", "Error, unable to update status for record");
            }
                    
            try { if ( DamsTools.getDamsConn() != null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.getCisConn() != null)  DamsTools.getCisConn().commit(); } catch (Exception e) { e.printStackTrace(); }   
            
        }  
    }
    
    // Method: populateCdisMapListToLink()
    // Purpose: Populates the list of CdisMap records that require linking using the criteria in the xml file
    private boolean populateCdisMapListToLink() {
        
        String sql = XmlUtils.returnFirstSqlForTag("retrieveMapIds");          
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
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
    private IdentifierType returnCorrespondingCisRecord(DamsRecord damsRecord) {
        
        IdentifierType cisIdentifierType = null;
        Connection dbConn = null;
        
        String sql = null;
        for(XmlData xmlInfo : DamsTools.getSqlQueryObjList()) { 
            if (! xmlInfo.getTag().equals("query")) {
                continue;
            }    
            xmlInfo.getDataValuesForAttribute("type","retrieveCisIds");         

            if (sql != null) {
                dbConn = DbUtils.returnDbConnFromString(xmlInfo.getAttributeData("dbConn"));  
                break;
            }       
        }
        if (sql == null) {
            logger.log(Level.FINEST, "retrieveCisId sql not found");
            return null;
        }   
        sql = damsRecord.replaceSqlVars(sql);        
        logger.log(Level.FINEST, "SQL: {0}", sql);
 
        if (dbConn == null) {
                logger.log(Level.FINEST, "Error: connection to db not found");
                return null;
        } 
        try (PreparedStatement stmt = dbConn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            if (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();            

                IdentifierFactory cisIdentFact = new IdentifierFactory();
                cisIdentifierType = cisIdentFact.identifierChooser(rsmd.getColumnLabel(1).toLowerCase());
                cisIdentifierType.setIdentifierValue(rs.getString(1));        
            }
            else {
                return null;
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain corresponding CIS Records", e);
            return null;
        }
        return cisIdentifierType;
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
        reqProps.add("lccIdType");
        reqProps.add("cis");
        reqProps.add("cisInstance");
        
        if (DamsTools.getProperty("cis").equals("tms")) {
            reqProps.add("updateTmsThumbnail");
        }
        
        if (DamsTools.getProperty("updateTmsThumbnail") != null && DamsTools.getProperty("updateTmsThumbnail").equals("true")  ) {
            reqProps.add("damsRepo");
        }
        
        //add more required props here
        return reqProps;    
    }
    
    public boolean requireSqlCriteria () {
        return true;
    }
      
}
