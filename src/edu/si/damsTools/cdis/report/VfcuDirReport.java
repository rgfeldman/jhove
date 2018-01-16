/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.report.attachment.DataSection;
import edu.si.damsTools.cdis.report.attachment.FailedSection;
import edu.si.damsTools.cdis.report.attachment.LinkedDamsSection;
import edu.si.damsTools.utilities.XmlQueryData;
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

    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList<String> multiReportKeyValues;
   
    public ArrayList<String> returnKeyValueList() {
        return this.multiReportKeyValues;
    }
     
    private String returnRptVendorDir(Integer md5FileId) {
        
        String sql = null;
        String vendorDir = null;
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8), SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '/', -1, 1)-1) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + md5FileId;
        } 
        else {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8), file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + md5FileId;
        }
                
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                if (rs.getString(2) == null) {
                    vendorDir = rs.getString(1).substring(rs.getString(1).lastIndexOf("/") + 1);
                }
                else {
                    vendorDir = rs.getString(1) + "/" + rs.getString(2);
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
        
        return DamsTools.getProjectCd().toUpperCase() + " CDIS Activity Report- " + returnRptVendorDir(Integer.valueOf(multiRptkeyValue) );
            
    }
    
    public String returnEmailToList () {
        return DamsTools.getProperty("vfcuDirEmailList");
    }
    
   public String returnEmailTitle(String multiRptkeyValue) {
       return DamsTools.getProjectCd().toUpperCase()+ ": Batch Hot Folder Import Activity Report - " + returnRptVendorDir(Integer.valueOf(multiRptkeyValue) );

    }
   
    
    public boolean returnSupressAttachFlag(String masterMd5FileId) {
        if ( DamsTools.getProperty("vfcuDirRptSupressAttch") != null && DamsTools.getProperty("vfcuDirRptSupressAttch").equals("true")  ) { 
           
            int numErrors = 0;
            
            //Check to see if there are any Failures.  IF there are any failures then do not supress
            String sql = "SELECT count(*) " +
                    "FROM vfcu_media_file a, " +
                    "     vfcu_error_log b " +
                    "WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                    "AND b.vfcu_md5_file_id = " + masterMd5FileId;
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
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
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","getMultiReportKeyValue");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "Error: Required sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {     
                multiReportKeyValues.add(rs.getString(1));
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
            return false; 
        }            
        return true;
    }
    
    public boolean updateDbComplete (String md5FileId) {
        
        int recordsUpdated;
        
        String sql = "UPDATE vfcu_md5_file " +
                     "SET cdis_rpt_dt = SYSDATE " +
                     "WHERE vfcu_md5_file_id = " + md5FileId;
        
       try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {
            
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update reportData", e );
            return false;
        }
        
        return true;
                
    }
    
    public String returnStatsListsHeader(String multiRptkeyValue) {
        String sql;
        String sdir = null;
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8) || '/' || SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '/', -1, 1)-1) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + multiRptkeyValue;
            } 
        else {
            sql = "SELECT SUBSTR(base_path_vendor,INSTR(base_path_vendor,'sources')+8) || '/' || file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + multiRptkeyValue;
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
                ResultSet rs = stmt.executeQuery() ) {

                if (rs.next()) {
                    sdir = rs.getString(1);
                }        
                else {
                    throw new Exception();
                }
              
            }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to count errors for report", e);
        }
            
        String sourceDir = "Source Directory: " + sdir;
        
        return sourceDir;
    }
          
}
