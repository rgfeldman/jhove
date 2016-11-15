/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TeamsLinks {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String destValue;
    private String linkType;
    private String srcValue;
    
    public String getDestValue() {
        return this.destValue == null ? "" : this.destValue;
    }
    
    public String getSrcValue() {
        return this.srcValue;
    }
    
    public String getLinkType() {
        return this.linkType;
    }
    
    public void setDestValue (String destValue) {
        this.destValue = destValue;
    }

    public void setLinkType (String linkType) {
        this.linkType = linkType;
    }
    
    public void setSrcValue (String srcValue) {
        this.srcValue = srcValue;
    }

    public boolean populateDestValueNotDeleted () {
        
        String sql = "SELECT dest_value " +
                    "FROM towner.teams_links a, " +
                    "       towner.uois b " +
                    "       towner.uois c " +
                    "WHERE a.src_value = b.uoi_id " +
                    "AND   a.dest_value = b.uoi_id " +
                    "AND   b.content_state != 'DELETED' " +
                    "AND   c.content_state != 'DELETED' " +
                    "AND   a.src_value = '" + getSrcValue() + "' " +
                    "AND   a.link_type = '" + getLinkType() + "' ";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setDestValue (rs.getString(1));
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain destValue from teams_links", e );
                return false;
        }
        return true;
    }
    
    public boolean createRecord () {
        
        int rowsInserted = 0;
        
        String sql =  "INSERT INTO towner.teams_links ( " +
                "src_type, " +
                "src_value, " +
                "dest_type, " +
                "dest_value, " +
                "link_type, " +
                "seq_num, " +
                "follow_latest) " +
             "VALUES ( " +
                "'UOI', " +
                "'" + getSrcValue() + "', " +
                "'UOI', " +
                "'" + getDestValue() + "', " +
                "'" + getLinkType() + "', " +
                0 + ", " +
                "'N')";
                
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
        
            rowsInserted = pStmt.executeUpdate();
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into TEAMS_LINKS table", e );
                return false;
        }      
        
        return true;
    }
}
