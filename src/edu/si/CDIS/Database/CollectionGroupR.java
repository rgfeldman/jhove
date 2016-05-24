/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CollectionGroupR {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String siHoldingUnit;
    
    public String getSiHoldingUnit() {
        return this.siHoldingUnit;
    }
    
    public boolean populateSiHoldingUnit () {
        
        String sql = "SELECT si_holding_unit " +
                    "FROM collection_group_r " +
                    "WHERE collection_group_cd = '" + CDIS.getCollectionGroup() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                siHoldingUnit = rs.getString(1);
            }   
            else {
                // we need a map id, if we cant find one then raise error
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain holding unit for collection code", e );
                return false;
        }
        return true;
    }
}
