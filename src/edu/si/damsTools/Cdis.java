/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools;

import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class Cdis implements AppBehavior {
    public String returnCapAppName() {
        return "CDIS";
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        //add more required props here
        
        return reqProps;
        
    }

}
