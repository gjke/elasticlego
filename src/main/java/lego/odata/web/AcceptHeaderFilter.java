package lego.odata.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import javax.servlet.http.HttpServletRequestWrapper;

public class AcceptHeaderFilter implements Filter {
	public void destroy() {

    }
	
	public void init(FilterConfig filterConfig) throws ServletException {

    }
	
	
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletRequestWrapper wrapper = new ChangeAcceptHeader(req);
        chain.doFilter(wrapper, response);
    }
	

}
