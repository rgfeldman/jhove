/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned;

import java.io.File;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author rfeldman
 */
public class xmlConfig {
    
    ArrayList<String>SelectStmt = new ArrayList<String>();
    
    public ArrayList<String> getSelectStmt() {
        return this.SelectStmt;
    }
    
    private void addSelectStmt (String SelectStmt) {
        this.SelectStmt.add(SelectStmt);
    }
        
    public void read() {
        //need to use reflection invoke

        
    
        System.out.println("In MetaData sync Redesigned code");
    
        try {
            //Class cls = Class.forName("CDIS_redesigned.MetaDataTarget");
            //Object obj = cls.newInstance();
            
            //Locate the metaData xml file
            File file = new File("conf\\metaData.xml");
        
            //Set up the doc which will hold the xml file
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);
            doc.getDocumentElement().normalize();

            System.out.println("Root element " + doc.getDocumentElement().getNodeName());

            // Look for the query tags
            NodeList nodeLst = doc.getElementsByTagName("query");
            
            String selectSql = null;
      
            // For each query tag found....
            for (int s = 0; s < nodeLst.getLength(); s++) {

                Node fstNode = nodeLst.item(s);
        
                if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
                    selectSql = fstNode.getTextContent();
                    
                    System.out.println("SQL: " + selectSql);
                    
                    if (selectSql != null ) { 
                        // populate objects attribute with the SQL that was obtained
                        addSelectStmt(selectSql);
                    }
                    
                }
            }
       
        } catch (Exception e) {
            e.printStackTrace();
        }   

    }
    //create the hash array in The default constructor

    
}
