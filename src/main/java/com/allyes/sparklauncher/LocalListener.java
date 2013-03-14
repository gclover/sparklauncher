package com.allyes.sparklauncher;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
 
import java.io.IOException;
import java.util.Set;
 
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
 
public class LocalListener implements Runnable {
	
	private int port;
	private Handler handler;
	
	private static class Handler extends AbstractHandler {
		
		private int minNodes;
		private int maxNodes;
		
		private String masterHost;
		private Set<String> acceptedHosts;
		private boolean masterSelected = false;
		private int currentNodes = 0;
		
		public Handler(int minNodes, int maxNodes) {
			this.minNodes = minNodes;
			this.maxNodes = maxNodes;
		}
		
		public void handle(String target, Request baseRequest,
        HttpServletRequest request, HttpServletResponse response) 
        		throws IOException, ServletException {
			
			String output = "reject:null";
			String remoteHost = request.getRemoteHost();
			if (request.getQueryString() == "/register") {
				if (!acceptedHosts.contains(remoteHost)) {
					if (!masterSelected) {
						masterSelected = true;
						masterHost = remoteHost;
						acceptedHosts.add(masterHost);
						output = "master:null";
					} else {
						if (currentNodes < maxNodes) {
							currentNodes += 1;
							acceptedHosts.add(remoteHost);
							output = "worker:" + masterHost;
						} else {
							output = "full:null";
						}
					}
				}
			} 
			
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			response.getWriter().println(output);
		}
	}

	public LocalListener(int port, int minNodes, int maxNodes) {
		this.port = port;
		this.handler = new Handler(minNodes, maxNodes);
	}
	
	public void run() {
		Server server = new Server(port);
    server.setHandler(handler);
    try {
			server.start();
			server.join();
    } catch (InterruptedException e) {
    	e.printStackTrace();
    } catch (Exception e) {
    	e.printStackTrace();
    }
	}
}