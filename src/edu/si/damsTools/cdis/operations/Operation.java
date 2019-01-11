/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;

import edu.si.damsTools.DamsTools;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public abstract class Operation {
    
    public abstract ArrayList<String> returnRequiredProps ();
     
    public abstract void invoke();
    
    public abstract boolean requireSqlCriteria () ;
}
