package br.com.sankhya.acoesgrautec.model;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "TGFFIN")
public class Financeiro {

    @Id
    @Column(name = "NUFIN")
    private BigDecimal nufin;

    @Column(name = "NUNOTA")
    private BigDecimal nunota;

    @Column(name = "NUMNOTA")
    private String numnota;

    @Column(name = "ORIGEM")
    private String origem;

    @Column(name = "RECDESP")
    private String recdesp;

    @Column(name = "CODEMP")
    private BigDecimal codemp;

    @Column(name = "CODCENCUS")
    private BigDecimal codcencus;

    @Column(name = "CODNAT")
    private BigDecimal codnat;

    @Column(name = "CODTIPOPER")
    private BigDecimal codtipoper;

    @Column(name = "DHTIPOPER")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dhtipoper;

    @Column(name = "CODTIPOPERBAIXA")
    private BigDecimal codtipoperbaixa;

    @Column(name = "DHTIPOPERBAIXA")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dhtipoperbaixa;

    @Column(name = "CODPARC")
    private BigDecimal codparc;

    @Column(name = "CODTIPTIT")
    private BigDecimal codtiptit;

    @Column(name = "VLRDESDOB")
    private BigDecimal vlrdesdob;

    @Column(name = "VLRDESC")
    private BigDecimal vlrdesc;

    @Column(name = "VLRBAIXA")
    private BigDecimal vlrbaixa;

    @Column(name = "CODBCO")
    private BigDecimal codbco;

    @Column(name = "CODCTABCOINT")
    private BigDecimal codctabcoint;

    @Column(name = "DTNEG")
    @Temporal(TemporalType.DATE)
    private Date dtneg;

    @Column(name = "DHMOV")
    @Temporal(TemporalType.TIMESTAMP)
    private Date dhmov;

    @Column(name = "DTALTER")
    @Temporal(TemporalType.DATE)
    private Date dtalter;

    @Column(name = "DTVENC")
    @Temporal(TemporalType.DATE)
    private Date dtvenc;

    @Column(name = "DTPRAZO")
    @Temporal(TemporalType.DATE)
    private Date dtprazo;

    @Column(name = "DTVENCINIC")
    @Temporal(TemporalType.DATE)
    private Date dtvencinic;

    @Column(name = "TIPJURO")
    private String tipjuro;

    @Column(name = "TIPMULTA")
    private String tipmulta;

    @Column(name = "HISTORICO")
    private String historico;

    @Column(name = "TIPMARCCHEQ")
    private String tipmarccheq;

    @Column(name = "AUTORIZADO")
    private String autorizado;

    @Column(name = "BLOQVAR")
    private String bloqvar;

    @Column(name = "INSSRETIDO")
    private String inssretido;

    @Column(name = "ISSRETIDO")
    private String issretido;

    @Column(name = "PROVISAO")
    private String provisao;

    @Column(name = "RATEADO")
    private String rateado;

    @Column(name = "TIMBLOQUEADA")
    private String timbloqueada;

    @Column(name = "IRFRETIDO")
    private String irfretido;

    @Column(name = "TIMTXADMGERALU")
    private String timtxadmgeralu;

    @Column(name = "VLRDESCEMBUT")
    private BigDecimal vlrdescembut;

    @Column(name = "VLRINSS")
    private BigDecimal vlrinss;

    @Column(name = "VLRIRF")
    private BigDecimal vlrirf;

    @Column(name = "VLRISS")
    private BigDecimal vlriss;

    @Column(name = "VLRJURO")
    private BigDecimal vlrjuro;

    @Column(name = "VLRJUROEMBUT")
    private BigDecimal vlrjuroembut;

    @Column(name = "VLRJUROLIB")
    private BigDecimal vlrjurolib;

    @Column(name = "VLRJURONEGOC")
    private BigDecimal vlrjuronegoc;

    @Column(name = "VLRMOEDA")
    private BigDecimal vlrmoeda;

    @Column(name = "VLRMOEDABAIXA")
    private BigDecimal vlrmoedabaixa;

    @Column(name = "VLRMULTA")
    private BigDecimal vlrmulta;

    @Column(name = "VLRMULTAEMBUT")
    private BigDecimal vlrmultaembut;

    @Column(name = "VLRMULTALIB")
    private BigDecimal vlrmultalib;

    @Column(name = "VLRMULTANEGOC")
    private BigDecimal vlrmultanegoc;

    @Column(name = "VLRPROV")
    private BigDecimal vlrprov;

    @Column(name = "VLRVARCAMBIAL")
    private BigDecimal vlrvarcambial;

    @Column(name = "VLRVENDOR")
    private BigDecimal vlrvendor;

    @Column(name = "ALIQICMS")
    private BigDecimal aliqicms;

    @Column(name = "BASEICMS")
    private BigDecimal baseicms;

    @Column(name = "CARTAODESC")
    private String cartaodesc;

    @Column(name = "CODMOEDA")
    private BigDecimal codmoeda;

    @Column(name = "CODPROJ")
    private BigDecimal codproj;

    @Column(name = "CODVEICULO")
    private BigDecimal codveiculo;

    @Column(name = "CODVEND")
    private BigDecimal codvend;

    @Column(name = "DESPCART")
    private BigDecimal despcart;

    @Column(name = "NUMCONTRATO")
    private BigDecimal numcontrato;

    @Column(name = "ORDEMCARGA")
    private BigDecimal ordemcarga;

    @Column(name = "CODUSU")
    private BigDecimal codusu;

    @Column(name = "AD_IDEXTERNO")
    private String adIdexterno;

    @Column(name = "AD_IDALUNO")
    private String adIdaluno;

    public String getAdIdaluno() {
        return adIdaluno;
    }

    public void setAdIdaluno(String adIdaluno) {
        this.adIdaluno = adIdaluno;
    }

    public BigDecimal getNufin() {
        return nufin;
    }

    public void setNufin(BigDecimal nufin) {
        this.nufin = nufin;
    }

    public BigDecimal getNunota() {
        return nunota;
    }

    public void setNunota(BigDecimal nunota) {
        this.nunota = nunota;
    }

    public String getNumnota() {
        return numnota;
    }

    public void setNumnota(String numnota) {
        this.numnota = numnota;
    }

    public String getOrigem() {
        return origem;
    }

    public void setOrigem(String origem) {
        this.origem = origem;
    }

    public String getRecdesp() {
        return recdesp;
    }

    public void setRecdesp(String recdesp) {
        this.recdesp = recdesp;
    }

    public BigDecimal getCodemp() {
        return codemp;
    }

    public void setCodemp(BigDecimal codemp) {
        this.codemp = codemp;
    }

    public BigDecimal getCodcencus() {
        return codcencus;
    }

    public void setCodcencus(BigDecimal codcencus) {
        this.codcencus = codcencus;
    }

    public BigDecimal getCodnat() {
        return codnat;
    }

    public void setCodnat(BigDecimal codnat) {
        this.codnat = codnat;
    }

    public BigDecimal getCodtipoper() {
        return codtipoper;
    }

    public void setCodtipoper(BigDecimal codtipoper) {
        this.codtipoper = codtipoper;
    }

    public Date getDhtipoper() {
        return dhtipoper;
    }

    public void setDhtipoper(Date dhtipoper) {
        this.dhtipoper = dhtipoper;
    }

    public BigDecimal getCodtipoperbaixa() {
        return codtipoperbaixa;
    }

    public void setCodtipoperbaixa(BigDecimal codtipoperbaixa) {
        this.codtipoperbaixa = codtipoperbaixa;
    }

    public Date getDhtipoperbaixa() {
        return dhtipoperbaixa;
    }

    public void setDhtipoperbaixa(Date dhtipoperbaixa) {
        this.dhtipoperbaixa = dhtipoperbaixa;
    }

    public BigDecimal getCodparc() {
        return codparc;
    }

    public void setCodparc(BigDecimal codparc) {
        this.codparc = codparc;
    }

    public BigDecimal getCodtiptit() {
        return codtiptit;
    }

    public void setCodtiptit(BigDecimal codtiptit) {
        this.codtiptit = codtiptit;
    }

    public BigDecimal getVlrdesdob() {
        return vlrdesdob;
    }

    public void setVlrdesdob(BigDecimal vlrdesdob) {
        this.vlrdesdob = vlrdesdob;
    }

    public BigDecimal getVlrdesc() {
        return vlrdesc;
    }

    public void setVlrdesc(BigDecimal vlrdesc) {
        this.vlrdesc = vlrdesc;
    }

    public BigDecimal getVlrbaixa() {
        return vlrbaixa;
    }

    public void setVlrbaixa(BigDecimal vlrbaixa) {
        this.vlrbaixa = vlrbaixa;
    }

    public BigDecimal getCodbco() {
        return codbco;
    }

    public void setCodbco(BigDecimal codbco) {
        this.codbco = codbco;
    }

    public BigDecimal getCodctabcoint() {
        return codctabcoint;
    }

    public void setCodctabcoint(BigDecimal codctabcoint) {
        this.codctabcoint = codctabcoint;
    }

    public Date getDtneg() {
        return dtneg;
    }

    public void setDtneg(Date dtneg) {
        this.dtneg = dtneg;
    }

    public Date getDhmov() {
        return dhmov;
    }

    public void setDhmov(Date dhmov) {
        this.dhmov = dhmov;
    }

    public Date getDtalter() {
        return dtalter;
    }

    public void setDtalter(Date dtalter) {
        this.dtalter = dtalter;
    }

    public Date getDtvenc() {
        return dtvenc;
    }

    public void setDtvenc(Date dtvenc) {
        this.dtvenc = dtvenc;
    }

    public Date getDtprazo() {
        return dtprazo;
    }

    public void setDtprazo(Date dtprazo) {
        this.dtprazo = dtprazo;
    }

    public Date getDtvencinic() {
        return dtvencinic;
    }

    public void setDtvencinic(Date dtvencinic) {
        this.dtvencinic = dtvencinic;
    }

    public String getTipjuro() {
        return tipjuro;
    }

    public void setTipjuro(String tipjuro) {
        this.tipjuro = tipjuro;
    }

    public String getTipmulta() {
        return tipmulta;
    }

    public void setTipmulta(String tipmulta) {
        this.tipmulta = tipmulta;
    }

    public String getHistorico() {
        return historico;
    }

    public void setHistorico(String historico) {
        this.historico = historico;
    }

    public String getTipmarccheq() {
        return tipmarccheq;
    }

    public void setTipmarccheq(String tipmarccheq) {
        this.tipmarccheq = tipmarccheq;
    }

    public String getAutorizado() {
        return autorizado;
    }

    public void setAutorizado(String autorizado) {
        this.autorizado = autorizado;
    }

    public String getBloqvar() {
        return bloqvar;
    }

    public void setBloqvar(String bloqvar) {
        this.bloqvar = bloqvar;
    }

    public String getInssretido() {
        return inssretido;
    }

    public void setInssretido(String inssretido) {
        this.inssretido = inssretido;
    }

    public String getIssretido() {
        return issretido;
    }

    public void setIssretido(String issretido) {
        this.issretido = issretido;
    }

    public String getProvisao() {
        return provisao;
    }

    public void setProvisao(String provisao) {
        this.provisao = provisao;
    }

    public String getRateado() {
        return rateado;
    }

    public void setRateado(String rateado) {
        this.rateado = rateado;
    }

    public String getTimbloqueada() {
        return timbloqueada;
    }

    public void setTimbloqueada(String timbloqueada) {
        this.timbloqueada = timbloqueada;
    }

    public String getIrfretido() {
        return irfretido;
    }

    public void setIrfretido(String irfretido) {
        this.irfretido = irfretido;
    }

    public String getTimtxadmgeralu() {
        return timtxadmgeralu;
    }

    public void setTimtxadmgeralu(String timtxadmgeralu) {
        this.timtxadmgeralu = timtxadmgeralu;
    }

    public BigDecimal getVlrdescembut() {
        return vlrdescembut;
    }

    public void setVlrdescembut(BigDecimal vlrdescembut) {
        this.vlrdescembut = vlrdescembut;
    }

    public BigDecimal getVlrinss() {
        return vlrinss;
    }

    public void setVlrinss(BigDecimal vlrinss) {
        this.vlrinss = vlrinss;
    }

    public BigDecimal getVlrirf() {
        return vlrirf;
    }

    public void setVlrirf(BigDecimal vlrirf) {
        this.vlrirf = vlrirf;
    }

    public BigDecimal getVlriss() {
        return vlriss;
    }

    public void setVlriss(BigDecimal vlriss) {
        this.vlriss = vlriss;
    }

    public BigDecimal getVlrjuro() {
        return vlrjuro;
    }

    public void setVlrjuro(BigDecimal vlrjuro) {
        this.vlrjuro = vlrjuro;
    }

    public BigDecimal getVlrjuroembut() {
        return vlrjuroembut;
    }

    public void setVlrjuroembut(BigDecimal vlrjuroembut) {
        this.vlrjuroembut = vlrjuroembut;
    }

    public BigDecimal getVlrjurolib() {
        return vlrjurolib;
    }

    public void setVlrjurolib(BigDecimal vlrjurolib) {
        this.vlrjurolib = vlrjurolib;
    }

    public BigDecimal getVlrjuronegoc() {
        return vlrjuronegoc;
    }

    public void setVlrjuronegoc(BigDecimal vlrjuronegoc) {
        this.vlrjuronegoc = vlrjuronegoc;
    }

    public BigDecimal getVlrmoeda() {
        return vlrmoeda;
    }

    public void setVlrmoeda(BigDecimal vlrmoeda) {
        this.vlrmoeda = vlrmoeda;
    }

    public BigDecimal getVlrmoedabaixa() {
        return vlrmoedabaixa;
    }

    public void setVlrmoedabaixa(BigDecimal vlrmoedabaixa) {
        this.vlrmoedabaixa = vlrmoedabaixa;
    }

    public BigDecimal getVlrmulta() {
        return vlrmulta;
    }

    public void setVlrmulta(BigDecimal vlrmulta) {
        this.vlrmulta = vlrmulta;
    }

    public BigDecimal getVlrmultaembut() {
        return vlrmultaembut;
    }

    public void setVlrmultaembut(BigDecimal vlrmultaembut) {
        this.vlrmultaembut = vlrmultaembut;
    }

    public BigDecimal getVlrmultalib() {
        return vlrmultalib;
    }

    public void setVlrmultalib(BigDecimal vlrmultalib) {
        this.vlrmultalib = vlrmultalib;
    }

    public BigDecimal getVlrmultanegoc() {
        return vlrmultanegoc;
    }

    public void setVlrmultanegoc(BigDecimal vlrmultanegoc) {
        this.vlrmultanegoc = vlrmultanegoc;
    }

    public BigDecimal getVlrprov() {
        return vlrprov;
    }

    public void setVlrprov(BigDecimal vlrprov) {
        this.vlrprov = vlrprov;
    }

    public BigDecimal getVlrvarcambial() {
        return vlrvarcambial;
    }

    public void setVlrvarcambial(BigDecimal vlrvarcambial) {
        this.vlrvarcambial = vlrvarcambial;
    }

    public BigDecimal getVlrvendor() {
        return vlrvendor;
    }

    public void setVlrvendor(BigDecimal vlrvendor) {
        this.vlrvendor = vlrvendor;
    }

    public BigDecimal getAliqicms() {
        return aliqicms;
    }

    public void setAliqicms(BigDecimal aliqicms) {
        this.aliqicms = aliqicms;
    }

    public BigDecimal getBaseicms() {
        return baseicms;
    }

    public void setBaseicms(BigDecimal baseicms) {
        this.baseicms = baseicms;
    }

    public String getCartaodesc() {
        return cartaodesc;
    }

    public void setCartaodesc(String cartaodesc) {
        this.cartaodesc = cartaodesc;
    }

    public BigDecimal getCodmoeda() {
        return codmoeda;
    }

    public void setCodmoeda(BigDecimal codmoeda) {
        this.codmoeda = codmoeda;
    }

    public BigDecimal getCodproj() {
        return codproj;
    }

    public void setCodproj(BigDecimal codproj) {
        this.codproj = codproj;
    }

    public BigDecimal getCodveiculo() {
        return codveiculo;
    }

    public void setCodveiculo(BigDecimal codveiculo) {
        this.codveiculo = codveiculo;
    }

    public BigDecimal getCodvend() {
        return codvend;
    }

    public void setCodvend(BigDecimal codvend) {
        this.codvend = codvend;
    }

    public BigDecimal getDespcart() {
        return despcart;
    }

    public void setDespcart(BigDecimal despcart) {
        this.despcart = despcart;
    }

    public BigDecimal getNumcontrato() {
        return numcontrato;
    }

    public void setNumcontrato(BigDecimal numcontrato) {
        this.numcontrato = numcontrato;
    }

    public BigDecimal getOrdemcarga() {
        return ordemcarga;
    }

    public void setOrdemcarga(BigDecimal ordemcarga) {
        this.ordemcarga = ordemcarga;
    }

    public BigDecimal getCodusu() {
        return codusu;
    }

    public void setCodusu(BigDecimal codusu) {
        this.codusu = codusu;
    }

    public String getAdIdexterno() {
        return adIdexterno;
    }

    public void setAdIdexterno(String adIdexterno) {
        this.adIdexterno = adIdexterno;
    }


}
