/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operation;

import java.sql.Connection;

/**
 *
 * @author rfeldman
 */
public class SendToHotFolder implements OperationBehavior {
    
    public Connection returnSrcDbConn(Connection cisConn, Connection damsConn) {
        return damsConn;
    }
    
    public Connection returnTgtDbConn(Connection cisConn, Connection damsConn) {
        return null;
    }
}
