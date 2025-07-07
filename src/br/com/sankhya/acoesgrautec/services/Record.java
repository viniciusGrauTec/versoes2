package br.com.sankhya.acoesgrautec.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Classe responsﾃ｡vel por inserir um registro em alguma tabela do Sankhya/Jiva.<br>
 * <br>
 * Observaﾃｧﾃｵes:<br>
 * - Campos do tipo texto sempre seraﾌバ envolvidos por um CDATA[]<br>
 * - O separador de decimais seraﾌ? sempre o ponto ('.')<br>
 * - Campos data teraﾌバ formato 'dd/MM/yyyy HH:mm:ss'. Para campos somente
 * data, as posicﾌｧoﾌテs relacionadas a horas, minutos e segundos seraﾌバ
 * substituiﾌ?dos por 00:00:00<br>
 * 
 * @author andrebuarque
 * @since 2018-01-11
 */
public class Record {

	private String entity;
	private Map<String, String> fields;
	private SWServiceInvoker service;

	public Record(String entity, SWServiceInvoker service) {
		this.entity = entity;
		this.fields = new HashMap<String, String>();
		this.service = service;
	}

	public void setString(String field, String value) {
		/*
		 * if (fields.containsKey(field)) fields.replace(field, value); else
		 * fields.put(field, value);
		 */

		fields.put(field, value);
	}

	public void setText(String field, String value) {
		/*
		 * if (fields.containsKey(field)) fields.replace(field,
		 * String.format("<![CDATA[%s]]", value)); else fields.put(field,
		 * String.format("<![CDATA[%s]]", value));
		 */
		fields.put(field, String.format("<![CDATA[%s]]", value));

	}

	public boolean save() throws Exception {
		service.setDebugMode();

		String body = this.getBody();

		Document doc = service.call("CRUDServiceProvider.saveRecord", "mge",
				body);

		return isRequestOK(doc);
	}

	private boolean isRequestOK(Document doc) {
		Node statusNode = doc.getElementsByTagName("serviceResponse").item(0)
				.getAttributes().getNamedItem("status");
		String status = statusNode.getTextContent().trim();

		return "1".equals(status);
	}

	private String getBody() {
		StringBuffer body = new StringBuffer();

		body.append("<dataSet rootEntity=\"" + entity + "\">")
				.append("<entity path=\"\">").append("<fieldset list=\"*\"/>")
				.append("</entity>").append("<dataRow>")
				.append("<localFields>");

		Set<Entry<String, String>> set = fields.entrySet();
		for (Entry<String, String> entry : set) {
			body.append(String.format("<%s>%s</%s>", entry.getKey(),
					entry.getValue(), entry.getKey()));
		}

		body.append("</localFields>").append("</dataRow>").append("</dataSet>");

		return body.toString();
	}

}
