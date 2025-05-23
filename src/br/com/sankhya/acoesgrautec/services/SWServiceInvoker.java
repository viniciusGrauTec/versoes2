package br.com.sankhya.acoesgrautec.services;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.codec.binary.Base64;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;

public class SWServiceInvoker {
	private String domain;
	private String user;
	private String md5 = "";
	private String pass;
	private boolean debug;
	private boolean silentMode;
	boolean modoDebug = true;
	private boolean useJson;
	private boolean criptedPass;

	public SWServiceInvoker(String domain, String user, String md5) {
		this.domain = domain;
		this.user = user;
		this.md5 = md5;
		this.debug = true;
	}

	public SWServiceInvoker(String domain, String user, String md5,
			boolean criptedPass) {
		this.domain = domain;
		this.user = user;
		this.md5 = md5;
		this.criptedPass = criptedPass;
		this.debug = true;
	}

	public SWServiceInvoker(ContextoAcao ctx) throws Exception {
		this.domain = "";
		System.out.println(String.format("",
				EnviromentUtils.getPort(ctx)));
		this.user = "sup";
		this.pass = EnviromentUtils.getSenhas(ctx);
	}

	public void setSilentMode(boolean silentMode) {
		this.silentMode = silentMode;
	}

	public void setDebugMode() {
		this.debug = false;
	}

	public Document call(String serviceName, String module, String body)
			throws Exception {

		String jsessionid;
		if (this.md5 != null)
			jsessionid = doLoginMd5();
		else
			jsessionid = doLogin();
		
		URLConnection conn = openConn(serviceName, module, jsessionid);
		
		System.out.println("Connect: " + conn);
		
		Document docResp = (Document) callService(conn, body, serviceName);

		doLogout(jsessionid);

		return docResp;
	}

	public JsonObject callAsJson(String serviceName, String module, String body)
			throws Exception {
		useJson = true;
		criptedPass = true;
		String jsessionid;
		if (this.md5 != null)
			jsessionid = doLoginMd5();
		else
			jsessionid = doLogin();

		URLConnection conn = openConn(serviceName, module, jsessionid);

		JsonObject docResp = (JsonObject) callService(conn, body, serviceName);

		doLogout(jsessionid);

		return docResp;
	}

	public Document call(String serviceName, String module, String body,
			String usuLogado) throws Exception {
		Object[] obj = EnviromentUtils.getDadosUser(usuLogado);
		this.user = (String) obj[0];
		this.md5 = (String) obj[1];
		this.criptedPass = true;
		String jsessionid = doLoginMd5();

		URLConnection conn = openConn(serviceName, module, jsessionid);

		Document docResp = (Document) callService(conn, body, serviceName);

		doLogout(jsessionid);

		return docResp;
	}
	
	public Document call(String serviceName, String module, String body,
			String usuLogado, String resourceId, String application) throws Exception {
		Object[] obj = EnviromentUtils.getDadosUser(usuLogado);
		this.user = (String) obj[0];
		this.md5 = (String) obj[1];
		this.criptedPass = true;
		String jsessionid = doLoginMd5();
		
		URLConnection conn = openConn(serviceName, module, jsessionid, resourceId, application);
		
		Document docResp = (Document) callService(conn, body, serviceName);
		
		doLogout(jsessionid);
		
		return docResp;
	}

	private String doLogin() throws Exception {
		URLConnection conn = openConn("MobileLoginSP.login", "mge", null);

		String session = null;

		if (useJson) {
			StringBuffer bodyBuf = new StringBuffer();
			if (getEmptyAsNull(user) != null) {
				bodyBuf.append(" NOMUSU: { $: ").append(user).append(" }, ");
			} else {
				bodyBuf.append(" NOMUSU: {}, ");
			}

			String interno = criptedPass ? "INTERNO" : "INTERNO";

			if (getEmptyAsNull(pass) != null) {
				bodyBuf.append(interno).append(": { $: ").append(pass)
						.append(" }");
			} else {
				bodyBuf.append(interno).append(": {}");
			}

			JsonObject docResp = (JsonObject) callService(conn,
					bodyBuf.toString(), "MobileLoginSP.login");
			if (docResp.has("responseBody")
					&& docResp.getAsJsonObject("responseBody")
							.has("jsessionid")) {
				session = docResp.getAsJsonObject("responseBody")
						.getAsJsonObject("jsessionid").get("$").getAsString()
						.trim();
			}
		} else {

			StringBuffer bodyBuf = new StringBuffer();

			bodyBuf.append("<NOMUSU>").append(user).append("</NOMUSU>");
			if (criptedPass) {
				bodyBuf.append("<INTERNO2>").append(pass).append("</INTERNO2>");
			} else {
				bodyBuf.append("<INTERNO>").append(pass).append("</INTERNO>");
			}

			Document docResp = (Document) callService(conn, bodyBuf.toString(),
					"MobileLoginSP.login");

			Node jsessionNode = (Node) xpath(docResp, "//jsessionid",
					XPathConstants.NODE);
			session = jsessionNode.getTextContent().trim();
		}

		return session;
	}

	private String doLoginMd5() throws Exception {
		URLConnection conn = openConn("MobileLoginSP.login", "mge", null);

		String session = null;

		if (useJson) {
			StringBuffer bodyBuf = new StringBuffer();
			if (getEmptyAsNull(user) != null) {
				bodyBuf.append(" NOMUSU: { $: ").append(user).append(" }, ");
			} else {
				bodyBuf.append(" NOMUSU: {}, ");
			}

			String interno = criptedPass ? "INTERNO2" : "INTERNO";

			if (getEmptyAsNull(md5) != null) {
				bodyBuf.append(interno).append(": { $: ").append(md5)
						.append(" }");
			} else {
				bodyBuf.append(interno).append(": {}");
			}
			
			System.out.println("Conn: " + conn);
			System.out.println("body: " + bodyBuf);
			
			JsonObject docResp = (JsonObject) callService(conn,
					bodyBuf.toString(), "MobileLoginSP.login");

			if (docResp.has("responseBody")
					&& docResp.getAsJsonObject("responseBody")
							.has("jsessionid")) {
				session = docResp.getAsJsonObject("responseBody")
						.getAsJsonObject("jsessionid").get("$").getAsString()
						.trim();
			}
		} else {
			StringBuffer bodyBuf = new StringBuffer();

			bodyBuf.append("<NOMUSU>").append(user).append("</NOMUSU>");
			if (criptedPass) {
				bodyBuf.append("<INTERNO2>").append(md5).append("</INTERNO2>");
			} else {
				bodyBuf.append("<INTERNO>").append(pass).append("</INTERNO>");
			}
			Document docResp = (Document) callService(conn, bodyBuf.toString(),
					"MobileLoginSP.login");

			Node jsessionNode = (Node) xpath(docResp, "//jsessionid",
					XPathConstants.NODE);

			session = jsessionNode.getTextContent().trim();
		}

		return session;
	}

	public static String getEmptyAsNull(String s) {
		if (s == null || s.length() == 0) {
			return null;
		}

		String trimed = s.trim();

		return trimed.length() == 0 ? null : trimed;
	}

	private void doLogout(String jsessionid) throws Exception {
		try {
			URLConnection conn = openConn("MobileLoginSP.logout", "mge",
					jsessionid);

			callService(conn, null, "MobileLoginSP.logout");
		} catch (Exception e) {
			e.printStackTrace(); // pode ser ignorado
		}
	}

	private void checkResultStatus(Node sr) throws Exception {
		Node statusNode = sr.getAttributes().getNamedItem("status");

		String status = statusNode.getTextContent().trim();

		if (!"1".equals(status) && !silentMode) {
			if (getChildNode("statusMessage", sr) != null) {
				String msg = getChildNode("statusMessage", sr).getTextContent();
				throw new Exception(decodeB64(msg.trim()));
			}
		}
	}

	private Node getChildNode(String name, Node parent) {
		NodeList l = parent.getChildNodes();

		for (int i = 0; i < l.getLength(); i++) {
			Node n = l.item(i);

			if (n.getNodeName().equalsIgnoreCase(name)) {
				return n;
			}
		}

		return null;
	}

	private String decodeB64(String s) {
		return new String(Base64.decodeBase64(s.getBytes()));
	}

	private Object xpath(Document d, String query, QName type) throws Exception {
		XPath xp = XPathFactory.newInstance().newXPath();

		XPathExpression xpe = xp.compile(query);
		return xpe.evaluate(d, type);
	}

	private void printNode(Node n) {
		if (debug) {
			System.out.println(n.toString());
		}

		NodeList l = n.getChildNodes();

		if (l != null && l.getLength() > 0) {
			for (int i = 0; i < l.getLength(); i++) {
				printNode(l.item(i));
			}
		}
	}

	private Object callService(URLConnection conn, String body,
			String serviceName) throws Exception {
		OutputStream out = null;
		InputStream inp = null;
		debug = false;
		try {
			out = conn.getOutputStream();
			OutputStreamWriter wout = new OutputStreamWriter(out, "ISO-8859-1");

			String requestBody = null;

			if (useJson) {
				requestBody = buildRequestBodyAsJson(body, serviceName);
			} else {
				requestBody = buildRequestBody(body, serviceName);
			}

			if (debug) {
				System.out.println(requestBody);
			}
			
			System.out.println("RequestBody do call: " + requestBody);
			
			wout.write(requestBody);

			wout.flush();

			inp = conn.getInputStream();

			if (useJson) {
				JsonObject doc = readInputStreamAsJsonObject(inp);

				/*
				 * 
				 * if (!doc.has("responseBody")) { //essa validao aqui est
				 * incorreta throw new
				 * Exception("Json de resposta no possui dados de resposta."); }
				 */

				return doc;

			} else {

				DocumentBuilderFactory dbf = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();

				Document doc = null;
				NodeList nodes = null;

				try {

					doc = db.parse(inp);

					if (debug) {
						printDocument(doc);
					}

					nodes = doc.getElementsByTagName("serviceResponse");

					if (debug) {
						printNode(nodes.item(0));
					}

					if (nodes == null || nodes.getLength() == 0) {
						throw new Exception(
								"XML de resposta não possui um elemento de resposta");
					}
				} catch (Exception e) {
					Exception error = new Exception(
							"Erro ao interpretar resposta do servidor");
					error.initCause(e);
					throw error;
				}
				
				
				
				checkResultStatus(nodes.item(0));
				
				System.out.println("Print doc login: " + doc);
				
				return doc;
			}
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (Exception e) {
				}
			}

			if (inp != null) {
				try {
					inp.close();
				} catch (Exception e) {
				}
			}
		}
	}

	private String buildRequestBody(String body, String serviceName) {
		StringBuffer buf = new StringBuffer();

		buf.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n");
		buf.append("<serviceRequest serviceName=\"").append(serviceName)
				.append("\">\n");
		buf.append("<requestBody>\n");
		buf.append(body == null ? "" : body);
		buf.append("</requestBody>\n");
		buf.append("</serviceRequest>");

		return buf.toString();
	}

	private String buildRequestBodyAsJson(String body, String serviceName) {
		StringBuffer buf = new StringBuffer();

		buf.append(" {");
		buf.append("    serviceName: ").append(serviceName).append(", ");
		buf.append("    requestBody: {");
		buf.append(body);
		buf.append("    }");
		buf.append(" }");

		return buf.toString();
	}

	private URLConnection openConn(String serviceName, String module,
			String sessionID) throws Exception {
		StringBuffer buf = new StringBuffer();

		buf.append(domain).append(domain.endsWith("/") ? "" : "/")
				.append(module == null ? "mge" : module).append("/service.sbr");

		/*
		 * if (sessionID != null) {
		 * buf.append(";JSESSIONID=").append(sessionID); }
		 */

		buf.append("?serviceName=").append(serviceName);

		if (sessionID != null) {
			buf.append("&mgeSession=").append(sessionID);
		}

		if (useJson) {
			buf.append("&outputType=json");
		}

		URL u = new URL(buf.toString());
		
		System.out.println("URL: " + u);
		
		URLConnection uc = u.openConnection();
		HttpURLConnection connection = (HttpURLConnection) uc;

		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("POST");
		connection.setRequestProperty("content-type", "text/xml");

		if (useJson) {
			connection.setRequestProperty("content-type", "application/json");
		} else {
			connection.setRequestProperty("content-type", "text/xml");
		}

		if (sessionID != null) {
			connection.setRequestProperty("Cookie", "JSESSIONID=" + sessionID);
		}

		connection.setRequestProperty("User-Agent", "SWServiceInvoker");
		
		System.out.println("Connection: " + connection);
		
		System.out.println("Conex�o: " +connection );
		
		return connection;
	}
	
	private URLConnection openConn(String serviceName, String module,
			String sessionID, String resourceId, String application) throws Exception {
		StringBuffer buf = new StringBuffer();
		
		buf.append(domain).append(domain.endsWith("/") ? "" : "/")
		.append(module == null ? "mge" : module).append("/service.sbr");
		
		/*
		 * if (sessionID != null) {
		 * buf.append(";JSESSIONID=").append(sessionID); }
		 */
		
		buf.append("?serviceName=").append(serviceName);
		
		if (!"".equals(application)) {
			buf.append("&application=").append(application);
		}
		
		if (sessionID != null) {
			buf.append("&mgeSession=").append(sessionID);
		}

		if (!"".equals(resourceId)) {
			buf.append("&resourceID=").append(resourceId);
		}
		
		if (useJson) {
			buf.append("&outputType=json");
		}
		
		URL u = new URL(buf.toString());
		//globalID=1196174C56AAE2268520D2DD4C522B2B& counter=54& &vss=1
		u = new URL("http://127.0.0.1:8501/mge/service.sbr?serviceName=BaixaFinanceiroSP.baixarTitulo&application=MovimentacaoFinanceira&preventTransform=false&mgeSession="+sessionID+"&resourceID=br.com.sankhya.fin.cad.movimentacaoFinanceira");
		
		System.out.println("URL: " + u);
		
		URLConnection uc = u.openConnection();
		HttpURLConnection connection = (HttpURLConnection) uc;
		
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setRequestMethod("POST");
		connection.setRequestProperty("content-type", "text/xml");
		
		if (useJson) {
			connection.setRequestProperty("content-type", "application/json");
		} else {
			connection.setRequestProperty("content-type", "text/xml");
		}
		
		if (sessionID != null) {
			connection.setRequestProperty("Cookie", "JSESSIONID=" + sessionID);
		}
		
		connection.setRequestProperty("User-Agent", "SWServiceInvoker");
		
		System.out.println("Connection: " + connection);
		
		System.out.println("Conex�o: " +connection );
		
		return connection;
	}

	private JsonObject readInputStreamAsJsonObject(InputStream is)
			throws IOException {
		JsonParser parser = new JsonParser();
		JsonObject bundle = null;
		FileInputStream fileStream = null;
		Reader reader = null;

		try {
			reader = new InputStreamReader(is, Charset.forName("UTF-8"));
			bundle = (JsonObject) parser.parse(reader);
		} catch (Exception e) {
			if (!(e instanceof FileNotFoundException)) {
				e.printStackTrace();
			}
		} finally {
			if (fileStream != null) {
				fileStream.close();
			}

			if (reader != null) {
				reader.close();
			}
		}

		return bundle;
	}

	public static void printDocument(Document doc) throws Exception {
		Transformer transformer = TransformerFactory.newInstance()
				.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		StreamResult result = new StreamResult(new StringWriter());
		DOMSource source = new DOMSource(doc);
		transformer.transform(source, result);
		String xmlString = result.getWriter().toString();
		System.out.println("----inicio---");
		System.out.println(xmlString);
		System.out.println("----fim-----");
	}

	public long printLogDebug(long tempoAnterior, String msgRetornoLog) {
		// Log Tempo
		if (modoDebug) {
			long tempoAgora = System.currentTimeMillis();
			long diffInSeconds = tempoAgora - tempoAnterior;
			// System.out.println(msgRetornoLog + " " +
			// retornarTempoLog(diffInSeconds));
			tempoAnterior = tempoAgora;
		}

		return tempoAnterior;
	}

	public String printLogErro(Class<?> clazz, long tempoAnterior) {
		// Log Tempo
		// String canonicalName = string.getCanonicalName();
		if (modoDebug) {
			// classeErro = getClass().getCanonicalName();

			long tempoAgora = System.currentTimeMillis();
			long diffInSeconds = tempoAgora - tempoAnterior;
			// System.out.println(msgRetornoLog + " " +
			// retornarTempoLog(diffInSeconds));
			tempoAnterior = tempoAgora;
		}

		return null;
	}

}
