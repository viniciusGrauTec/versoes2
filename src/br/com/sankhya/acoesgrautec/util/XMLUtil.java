package br.com.sankhya.acoesgrautec.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class XMLUtil {

	static final boolean INSERIR_LOG = true;

	public static boolean isNullOrEmpty(String campo) {
		return ((campo == null) || "".equals(campo.trim()));
	}

	/**
	 * Converte uma String em objeto que representa o XML.
	 * 
	 * @param c
	 * @param s
	 * @return
	 * @throws JAXBException
	 * @throws PropertyException
	 */
	public static Object stringToXml(Class<?> c, String s) throws JAXBException, PropertyException {
		JAXBContext jaxbContext = JAXBContext.newInstance(c);

		Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

		return jaxbUnmarshaller.unmarshal(new InputSource(new StringReader(s)));
	}

	/**
	 * Converte um objeto que representa XML numa String.
	 * 
	 * @param o
	 * @return
	 * @throws JAXBException
	 * @throws PropertyException
	 */
	public static String xmlToString(Object o) throws JAXBException, PropertyException {
		JAXBContext jaxbContext = JAXBContext.newInstance(o.getClass());
		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		jaxbMarshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);

		StringWriter sw = new StringWriter();
		jaxbMarshaller.marshal(o, sw);

		return sw.toString();
	}

	public static String xmlToString(Document doc) throws TransformerFactoryConfigurationError, TransformerException {
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		StreamResult result = new StreamResult(new StringWriter());
		DOMSource source = new DOMSource(doc);
		transformer.transform(source, result);
		String xmlString = result.getWriter().toString();
		return xmlString;
	}

	public static void saveXml(String conteudo, String vDir, String vArq) throws Exception {     
		String nomeServidor = "";
		try {
			if (vDir != null) {
				File diretorio = new File(vDir);     
				if (!diretorio.exists()) {   
					diretorio.mkdirs();     
				}  
			}

			Writer file = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(vArq),"ISO-8859-1"));     
			file.write(conteudo);     
			file.close();     
		} catch (Exception e) {
			throw new Exception(e.toString()); 
		}
	}

	public static String retornaAtributoXml(File file, String Tag, String Atributo) {
		String valor = "";
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.parse(file);

			NodeList nodes = doc.getElementsByTagName(Tag);

			for (int i = 0; i < nodes.getLength(); i++) {
				Element elemento = (Element) nodes.item(i);

				NodeList tag = elemento.getElementsByTagName(Atributo);
				Element linha = (Element) tag.item(0);
				valor = linha.getTextContent();			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return valor;		
	}

	public static String retornaTagRaizXml(String xml) throws Exception { 
		try {			
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
			DocumentBuilder builder = factory.newDocumentBuilder();  
			org.w3c.dom.Document document = builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8"))); 
			String ret = document.getDocumentElement().getNodeName().toString();
			return ret;
		} catch (Exception e) {
			throw new Exception(e.toString()); 
		}
	}

	public static void deletarLog() throws MGEModelException, SQLException {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();
			String sql = " delete AD_LOGCHEDULE ";
			pstmt = jdbc.getPreparedStatement(sql);
			pstmt.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
			throw new MGEModelException(e.getMessage());
		} finally {
			pstmt.close();
			jdbc.closeSession();
		}
	}

	public static void inserirLog(String log) throws MGEModelException,
	SQLException {

		if (!XMLUtil.INSERIR_LOG)
			return;

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();
			//	String sql = " insert into AD_LOGCHEDULE (ID, LOGCLOB) values ((select nvl(max(ID+1), 1) from AD_LOGCHEDULE), ?)";
			String sql = " insert into AD_LOGCHEDULE (ID, LOG, LOGCLOB) values ((select nvl(max(ID+1), 1) from AD_LOGCHEDULE), (select 'Sequ�ncia - '||nvl(max(ID+1),1)||' - '||to_char(sysdate,'dd/MM/yyyy hh24:mm:ss') from AD_LOGCHEDULE), ?)";
			pstmt = jdbc.getPreparedStatement(sql);
			pstmt.setString(1, log);
			pstmt.executeUpdate();

		} catch (Exception e) {
			e.printStackTrace();
			throw new MGEModelException(e.getMessage());
		} finally {
			pstmt.close();
			jdbc.closeSession();
		}
	}

	public String retornarInformacaoPorTagCampo(File arquivo, String tagPaiName, String tagFilhoCampo) {
		String cod = null;
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(arquivo);
			doc.getDocumentElement().normalize();
			NodeList nodes = doc.getElementsByTagName(tagPaiName);

			for (int i = 0; i < nodes.getLength(); i++) {
				Node node = nodes.item(i);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					cod = retornarValorPorTag(tagFilhoCampo, element);
					if (!XMLUtil.isNullOrEmpty(cod)) {
						break;
					}
				} // node.getNodeType() == Node.ELEMENT_NODE)
			} // for (int i = 0; i < nodes.getLength(); i++)
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cod;
	}

	public static String retornarValorPorTag(String tagCampo, Element element) {
		String retorno = null;
		try {
			NodeList nodes = null;
			if (element.getElementsByTagName(tagCampo).item(0) != null) {
				nodes = element.getElementsByTagName(tagCampo).item(0).getChildNodes();
			}
			if (nodes != null) {
				Node node = (Node) nodes.item(0);
				if (node != null) {
					retorno = node.getNodeValue();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retorno;
	}

	public static InputStream retornaArquivoInputStream(String caminhoarquivo) {			
		InputStream is = null;
		File file = new File(caminhoarquivo);
		// algumas variaveis
		int tam = 10;
		int retorno = 0;
		int inicio = 0;
		int fim = tam;
		int length = (int)file.length();
		// byte[] que guarda tudo
		byte bytes[] = new byte[(int)file.length()];
		try {
			// apontei para o arquivo
			is = new FileInputStream(file);
			// enquanto nao chegar ao fim...
			while (  (retorno = is.read(bytes, inicio, fim)) > 0 ) {
				// aqui o inicio Ã© aumentado pra onde parou da ultima vez...
				// o fim Ã© aumentado para mais "10" depois so inicio
				inicio += retorno;
				fim = inicio + tam;
			}
			// mostra como ficou a ultima iteracao
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("pro de " + inicio + " ate " +  fim);
		}
		return is;
	}

	public static String formatNumero(String num, Integer casasDecimais){
		String dec = num.substring(num.length() - casasDecimais);
		String intnum = num.substring(0, num.length() - casasDecimais);
		return intnum.concat(",").concat(dec);
	}

	public static String formatNumeroDI(String num){
		String retorno = "";
		String dec = num.substring(num.length() - 1);
		String intnum = num.substring(0, num.length() - 1);
		retorno =  intnum.concat("-").concat(dec);
		int qtdeDI = retorno.length();
		retorno = retorno.substring(0,2) + "/" + retorno.substring(2,qtdeDI);
		return retorno;
	}

	public static String formatDataBR(String data){
		String retorno = "";
		System.out.println(data);
		int tam = data.length();
		if (tam == 8) {
			//	System.out.println(data.substring(tam-2,tam) + "/" + data.substring(tam-4,tam-2)+ "/" + data.substring(0,tam-4));
			retorno = data.substring(tam-2,tam) + "/" + data.substring(tam-4,tam-2)+ "/" + data.substring(0,tam-4);
		}

		return retorno;
	}

	public static String lerXML(String fileXML) throws IOException {  
		String linha = "";  
		StringBuilder xml = new StringBuilder();  

		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileXML),"UTF-8"));  
		while ((linha = in.readLine()) != null) {  
			xml.append(linha);  
		}  
		in.close();  

		String arqXml = xml.toString();
		arqXml = arqXml.trim().replaceFirst("^([\\W]+)<","<");
		arqXml = arqXml.replace("'", " ").replace("  ", "");
		
		System.out.println(arqXml);
		return arqXml;  
	}

	public static String removeAcentos(String str) {  
		CharSequence cs = new StringBuilder(str == null ? "" : str);  
		return Normalizer.normalize(cs, Normalizer.Form.NFKD).replaceAll("\\p{InCombiningDiacriticalMarks}+", "");  
	}	
}

