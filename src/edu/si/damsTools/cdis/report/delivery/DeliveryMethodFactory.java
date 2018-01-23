/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.DamsTools;
/**
 *
 * @author rfeldman
 */
public class DeliveryMethodFactory {
    public DeliveryMethodFactory() {
        
    }
    
    public DeliveryMethod deliveryMeothodChooser() {
        
        DeliveryMethod deliveryMethod = null;
        
        switch (DamsTools.getProperty("rptDeliveryMethod")) {
            case "email" :
                deliveryMethod = new Email();
                break;
            case "sftp" :
                deliveryMethod = new Sftp();
                break;   
        }       
        return deliveryMethod;
    }
}
