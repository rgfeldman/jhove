/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.utilities.XmlData;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rfeldman
 */
public class LinkedCisSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList sectionTextData;
     
    public LinkedCisSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "in DAMS Linked to a prior-Existing CIS Record";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In CisLinked Section");  
        return "getCisLinkedRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no records to Link";
    }
    
    public boolean generateTextForRecord(Integer recordId) {
        
        CdisMap cdisMap = new CdisMap();
        cdisMap.setCdisMapId(recordId);
        cdisMap.populateMapInfo();
        
        SiAssetMetadata siAsst = new SiAssetMetadata();
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();  
        
        String rptInfo = getCisHierReportInfo(cdisMap);
        if (rptInfo != null ) {
            sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Linked To " + rptInfo);
        }
            
        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
     // Method: populateCdisMapListToLink()
    // Purpose: Populates the list of CdisMap records that require linking using the criteria in the xml file
    private String getCisHierReportInfo(CdisMap cdisMap) {
        
        String sql = XmlUtils.returnFirstSqlForTag("getCisLinkedRptInfo");    
        if (sql == null) {
            logger.log(Level.FINEST, "getCisLinkedRptInfo sql not found");
            return null;
        }
        
        String rptInfo = null;
        if (sql.contains("?CISID")) {
            Pattern p = Pattern.compile("\\?CISID-([A-Z][A-Z][A-Z])\\?");
            Matcher m = p.matcher(sql);
            
            while (m.find()) {
                
                CdisCisIdentifierMap cdisCisIdentifier = new CdisCisIdentifierMap();
                cdisCisIdentifier.setCdisMapId(cdisMap.getCdisMapId());
                cdisCisIdentifier.setCisIdentifierCd(m.group(1).toLowerCase());
                cdisCisIdentifier.populateCisIdentifierValueForCdisMapIdType(); 
     
                if (cdisCisIdentifier.getCisIdentifierValue() != null) {
                    sql = sql.replace("?CISID-" + m.group(1) + "?", cdisCisIdentifier.getCisIdentifierValue());       
                }
                else {
                    sql = sql.replace("?CISID-" + m.group(1) + "?", "");   
                }
            }
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getCisConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            if (rs.next()) {
                rptInfo = rs.getString(1);
            }
        }
        catch(Exception e) {
            logger.log(Level.FINEST, "Error, unable to obtain list of Map IDs to integrate", e);
            return "";
        }
        
        return rptInfo;
    }
       
}
