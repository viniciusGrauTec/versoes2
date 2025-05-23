package br.com.sankhya.acoesgrautec.callback;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesgrautec.model.CentralEvents;
import br.com.sankhya.acoesgrautec.model.Note;
import br.com.sankhya.acoesgrautec.util.LogCatcher;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.Jape;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.CentralFaturamento;
import br.com.sankhya.modelcore.comercial.ConfirmacaoNotaHelper;
import br.com.sankhya.modelcore.custommodule.ICustomCallBack;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.util.troubleshooting.SKError;
import br.com.sankhya.util.troubleshooting.TSLevel;

public class CallbackConfirmaPedido implements ICustomCallBack{

	@Override
	public Object call(final String event, final Map<String, Object> fields) {
		JapeSession.SessionHandle hnd = null;
		
		StringBuilder erro = null;
		
		try {
				if (CentralEvents.EVENTO_BEFORE_CENTRAL.getDescription().equals(event) || CentralEvents.EVENTO_BEFORE_PORTAL.getDescription().equals(event)) {
					//LogCatcher.logInfo(fields.get("nunota").toString());
					
					System.out.println("Entrou no if");
					
					System.out.println("Print do Fields: " + fields);
					
					Long nunota = Long.parseLong(fields.get("nunota").toString());
					
					System.out.println("Teste Nunota: " + nunota);
					
					JapeWrapper notaDAO = JapeFactory
							.dao(DynamicEntityNames.CABECALHO_NOTA);
					DynamicVO note = notaDAO
							.findByPK(nunota);
					
					JapeSession.close(hnd);
					
					System.out.println("Passou do VO: " + note);
					
					BigDecimal codparc = note.asBigDecimal("CODPARC");
					
					System.out.println("codparc: " + codparc);
					
					System.out.println("Nunota: " + nunota);
					System.out.println("Parceiro: " + codparc);
					
				}
					
		} catch (Exception e) {
			System.out.println(erro);
			e.printStackTrace();
		} finally {
			JapeSession.close(hnd);
		}

		return null;
	}
	
}