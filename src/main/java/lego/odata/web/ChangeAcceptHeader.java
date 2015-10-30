package lego.odata.web;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class ChangeAcceptHeader extends HttpServletRequestWrapper {

	private final String goodAcceptHeader = "odata.metadata=minimal";
	private final String oDataMetadataFullHeader = "odata.metadata=full";
	
	public ChangeAcceptHeader (HttpServletRequest request){
		super(request);
	}
	
	public Enumeration getHeaders(String name){
		Vector <String> headersVector = new Vector <String>();
		for (Enumeration<?> headers = super.getHeaders(name); headers.hasMoreElements();) {
			String value = (String) headers.nextElement();
			if ((name.equals("accept")) && (value.contains(oDataMetadataFullHeader))){
				value = replaceBadHeader(value);
				headersVector.add(value);
			}
			else{
				headersVector.add(value);
			}
		}     
		//System.out.println(name + " " + super.getHeader(name));
		//return (name.equals("accept")) ? goodAcceptHeader : super.getHeader(name);
		return headersVector.elements();
	}
	
	public String replaceBadHeader(String value){
		return value.replace(oDataMetadataFullHeader, goodAcceptHeader);
	}
}
