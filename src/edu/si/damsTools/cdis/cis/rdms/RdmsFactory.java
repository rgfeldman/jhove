/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.rdms;

import edu.si.damsTools.utilities.XmlUtils;

/**
 *
 * @author rfeldman
 */
public class RdmsFactory {
    
    public RdmsFactory() {
        
    }
    
    public Rdms rdmsChooser() {
        
        Rdms rdms = null;
        
        switch ( XmlUtils.getConfigValue("cisRdms") ) {

            case "mySql" :
                rdms = new MySql();
                break;
            case "oracle" :
                rdms = new Oracle();
                break;
            case "sqlServer" :
                rdms = new SqlServer();
                break;
            case "sybase" :
                rdms = new Sybase();
                break;
        }
        
        return rdms;
    }
}
