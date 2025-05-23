package br.com.sankhya.acoesgrautec.model;

public enum CentralEvents {
	EVENTO_BEFORE_CENTRAL("central.processarConfirmacao.before"),
	EVENTO_AFTER_CENTRAL("central.processarConfirmacao.after"),

	EVENTO_BEFORE_PORTAL("central.confirmacao_portal.before"),
	EVENTO_AFTER_PORTAL("central.confirmacao_portal.after");
	
	private final String description;

	CentralEvents(String description) {
		this.description = description;
	}

	public String getDescription() {
		return description;
	}
	
	
	
}
