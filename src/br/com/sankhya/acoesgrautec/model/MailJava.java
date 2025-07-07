package br.com.sankhya.acoesgrautec.model;

import java.util.List;
import java.util.Map;

public class MailJava {
	// indica se o formato de texto ser&aacute; texto ou html
	public static final String TYPE_TEXT_PLAIN = "text/plain";
	public static final String TYPE_TEXT_HTML = "text/html";
	// indica qual sera o servidor de email(gmail, hotmail...)
	private String smtpHostMail = "smtp.office365.com";
	// indica a porta de acesso ao servidor
	private String smtpPortMail = "587";
	// indica que a necessidade de autenticacao
	// no servidor(true ou false)
	private String smtpAuth = "true";
	// indica ao servidor que ele esta recebendo uma conexao segura
	private String smtpStarttls = "true";
	// nome do remetente do email
	private String fromNameMail = "TBM Textil";
	// email do remetente
	private String userMail = "mailbot2@tbmtextil.com.br";
	// senha do email do remetente
	private String passMail = "mailbotprdc0605";
	//Email do Remetente de Resposta
	private String mailReplyUser = "";
	// assunto do email
	private String subjectMail;
	// corpo do email, onde esta o texto da mensagem
	private String bodyMail;
	// lista com email e nome dos destinatarios
	private Map<String, String> toMailsUsers;
	// lista contendo os arquivos anexos
	private List<String> fileMails;
	// charset, no caso de html e necessario
	private String charsetMail = "ISO-8859-1";
	// tipo do formato da mensagem, texto ou html
	private String typeTextMail = TYPE_TEXT_HTML;

	public String getSmtpHostMail() {
		return smtpHostMail;
	}

	public void setSmtpHostMail(String smtpHostMail) {
		this.smtpHostMail = smtpHostMail;
	}

	public String getSmtpPortMail() {
		return smtpPortMail;
	}

	public void setSmtpPortMail(String smtpPortMail) {
		this.smtpPortMail = smtpPortMail;
	}

	public String getSmtpAuth() {
		return smtpAuth;
	}

	public void setSmtpAuth(String smtpAuth) {
		this.smtpAuth = smtpAuth;
	}

	public String getSmtpStarttls() {
		return smtpStarttls;
	}

	public void setSmtpStarttls(String smtpStarttls) {
		this.smtpStarttls = smtpStarttls;
	}

	public String getFromNameMail() {
		return fromNameMail;
	}

	public void setFromNameMail(String fromNameMail) {
		this.fromNameMail = fromNameMail;
	}

	public String getUserMail() {
		return userMail;
	}

	public void setUserMail(String userMail) {
		this.userMail = userMail;
	}

	public String getPassMail() {
		return passMail;
	}

	public void setPassMail(String passMail) {
		this.passMail = passMail;
	}

	public String getSubjectMail() {
		return subjectMail;
	}

	public void setSubjectMail(String subjectMail) {
		this.subjectMail = subjectMail;
	}

	public String getBodyMail() {
		return bodyMail;
	}

	public void setBodyMail(String bodyMail) {
		this.bodyMail = bodyMail;
	}

	public Map<String, String> getToMailsUsers() {
		return toMailsUsers;
	}

	public void setToMailsUsers(Map<String, String> toMailsUsers) {
		this.toMailsUsers = toMailsUsers;
	}

	public List<String> getFileMails() {
		return fileMails;
	}

	public void setFileMails(List<String> fileMails) {
		this.fileMails = fileMails;
	}

	public String getCharsetMail() {
		return charsetMail;
	}

	public void setCharsetMail(String charsetMail) {
		this.charsetMail = charsetMail;
	}

	public String getTypeTextMail() {
		return typeTextMail;
	}

	public void setTypeTextMail(String typeTextMail) {
		this.typeTextMail = typeTextMail;
	}

	public String getMailReplyUser() {
		return mailReplyUser;
	}

	public void setMailReplyUser(String mailReplyUser) {
		this.mailReplyUser = mailReplyUser;
	}
}
