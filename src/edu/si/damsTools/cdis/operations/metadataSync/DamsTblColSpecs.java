/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class DamsTblColSpecs {

     /*  Method :        populateColumnWidthArray
        Returns:        true for success, false for failures
        Description:    populates structure to hold columns and widths for all DAMS tables involved in metadata sync
    */
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    
    private final String tableName;
    private final HashMap<String,Integer> columnLengthMap;
    
    public DamsTblColSpecs(String tableName) {
        this.tableName = tableName;
        this.columnLengthMap = new HashMap<>();
    }
     
    
    public boolean populateColumnWidthArray () {
          
        String sql = "SELECT column_name, char_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + tableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type in ('VARCHAR2','CHAR') " + 
                     "AND column_name NOT IN ('UOI_ID','OWNING_UNIT_UNIQUE_NAME')" +
                     "UNION " +
                     "SELECT column_name, data_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + tableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type = 'NUMBER' " + 
                     "UNION " +
                     "SELECT column_name, 16 " +
                     "FROM all_tab_columns " +
                     "WHERE table_name = '" + tableName + "' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type = 'DATE' ";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs != null && rs.next()) {         
                columnLengthMap.put(rs.getString(1), rs.getInt(2));
            }   
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain data field lengths ", e);
            return false;
        }
        return true;
    }
}
