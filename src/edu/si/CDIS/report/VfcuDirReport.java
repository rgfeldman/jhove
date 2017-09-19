/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.report;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.VFCUMd5File;
import edu.si.CDIS.report.attachment.DataSection;
import edu.si.CDIS.report.attachment.FailedSection;
import edu.si.CDIS.report.attachment.LinkedDamsSection;
import edu.si.Utils.XmlSqlConfig;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rfeldman
 */
public class VfcuDirReport implements DisplayFormat {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private ArrayList<String> multiReportKeyValues;
   
    public ArrayList<String> returnKeyValueList() {
        return this.multiReportKeyValues;
    }
     
    private String returnRptVendorDir(Integer masterMd5FileId) {
        
        String sql = null;
        String vendorDir = null;
        
        if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
            sql = "SELECT SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '/', 1, 1)-1), base_path_vendor " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + masterMd5FileId;
        } 
        else {
            sql = "SELECT file_path_ending, base_path_vendor " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + masterMd5FileId;
        }
                
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                if (rs.getString(1) == null) {
                    vendorDir = rs.getString(2).substring(rs.getString(2).lastIndexOf("/") + 1);
                }
                else {
                    String strFilePathEnding = rs.getString(1);
                    //Add the records to the masterMd5Id list 
                    if (strFilePathEnding.contains("/")) {
                        vendorDir = strFilePathEnding.replace("/", "-");
                    }
                    else {
                        vendorDir = strFilePathEnding;
                    }
                }
            }        
            else {
                throw new Exception();
            }
        }
        catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain vendor dir for report", e);
	}
        
        return vendorDir;
    }
        
    
    public VfcuDirReport() {
       multiReportKeyValues = new ArrayList();
       logger.log(Level.FINEST,"Creating VFCUDir-based object"); 
    }

    
    public String returnDocHeader(String multiRptkeyValue) {
        
        return CDIS.getProjectCd().toUpperCase() + " CDIS Activity Report- " + returnRptVendorDir(Integer.valueOf(multiRptkeyValue) );
            
    }
    
    public String returnEmailToList () {
        return CDIS.getProperty("vfcuDirEmailList");
    }
    
    public String returnEmailTitle(String multiRptkeyValue) {
       return CDIS.getProjectCd().toUpperCase()+ ": Batch Hot Folder Import Activity Report - " + returnRptVendorDir(Integer.valueOf(multiRptkeyValue) );

    }
    
    public boolean returnSupressAttachFlag(String masterMd5FileId) {
        if ( CDIS.getProperty("vfcuDirRptSupressAttch") != null && CDIS.getProperty("vfcuDirRptSupressAttch").equals("true")  ) { 
           
            int numErrors = 0;
            
            //Check to see if there are any Failures.  IF there are any failures then do not supress
            String sql = "SELECT count(*) " +
                    "FROM vfcu_media_file a, " +
                    "     vfcu_error_log b " +
                    "WHERE a.vfcu_media_file_id = b.vfcu_media_file_id" +
                    "AND b.vfcu_md5_file_id = " + masterMd5FileId;
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = stmt.executeQuery() ) {

                if (rs.next()) {
                        if (rs.getString(1) == null) {
                        numErrors = rs.getInt(1);
                    }
                }        
                else {
                    throw new Exception();
                }
              
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to count errors for report", e);
            }
            
            if (numErrors == 0 ) {
                return true;
            }
        }
        
        return false;
    }
    
    public List<DataSection> sectionFactory() {
        
        List<DataSection> sections = new ArrayList<>();
        
        logger.log(Level.FINEST,"In VFCUDIR SectionFactory"); 
        
        sections.add(new FailedSection());
        sections.add(new LinkedDamsSection());
        
        return sections;
    }
    
    public boolean populateMultiReportKeyValues() {
        
        XmlSqlConfig xml = new XmlSqlConfig();
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        xml.setProjectCd(CDIS.getProjectCd());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getMultiReportKeyValue"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }  
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                while (rs.next()) {     
                    multiReportKeyValues.add(rs.getString(1));
                }
            }
            catch(Exception e) {
                logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
                return false; 
            }
        }            
        
        return true;
    }
    
    public boolean updateDbComplete (String masterMd5FileId) {
        
        int recordsUpdated;
        
        String sql = "UPDATE vfcu_md5_file " +
                     "SET cdis_rpt_dt = SYSDATE " +
                     "WHERE master_md5_file_id = " + masterMd5FileId;
        
       try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql) ) {
            
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update reportData", e );
            return false;
        }
        
        return true;
                
    }
    
}
