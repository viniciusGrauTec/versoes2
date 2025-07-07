package br.com.sankhya.acoesgrautec.jobs;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.jape.vo.DynamicVO;
import com.google.gson.JsonObject;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AtualizacaoCadastralAluno {

    private final JapeWrapper alunoDAO;
    private final EnviromentUtils util;

    public AtualizacaoCadastralAluno() {
        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
        this.alunoDAO = JapeFactory.dao("AD_ALUNOS");
        this.util = new EnviromentUtils();
    }

    public void verificarEAtualizarAluno(JsonObject jsonObject, String alunoId, BigDecimal codEmp) throws Exception {
        DynamicVO alunoExistente = buscarAlunoExistente(alunoId, codEmp);
        
        if (alunoExistente != null && alunoPrecisaAtualizar(alunoExistente, jsonObject)) {
            updateAlunoCompleto(
                alunoId,
                jsonObject.get("aluno_endereco").getAsString(),
                jsonObject.get("aluno_endereco_cep").getAsString(),
                jsonObject.get("aluno_endereco_bairro").getAsString(),
                jsonObject.get("aluno_endereco_cidade").getAsString(),
                jsonObject.get("aluno_endereco_uf").getAsString(),
                jsonObject.get("aluno_data_nascimento").getAsString(),
                jsonObject.get("aluno_telefone_celular").getAsString(),
                jsonObject.get("aluno_email").getAsString(),
                jsonObject.getAsJsonArray("cursos").get(0).getAsJsonObject().get("situacao_id").getAsString(),
                jsonObject.getAsJsonArray("cursos").get(0).getAsJsonObject().get("situacao_descricao").getAsString(),
                codEmp
            );
        }
    }

    private DynamicVO buscarAlunoExistente(String idExterno, BigDecimal codEmp) throws Exception {
        String filtro = "ID_EXTERNO = :idExterno AND CODEMP = :codEmp";
        Map<String, Object> parametros = new HashMap<>();
        parametros.put("idExterno", idExterno);
        parametros.put("codEmp", codEmp);
        
        return alunoDAO.findOne(filtro, parametros);
    }

    private boolean alunoPrecisaAtualizar(DynamicVO alunoExistente, JsonObject jsonAluno) throws ParseException {
        SimpleDateFormat formatoBanco = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoAPI = new SimpleDateFormat("yyyy-MM-dd");
        
        if (!Objects.equals(alunoExistente.asString("ENDERECO"), jsonAluno.get("aluno_endereco").getAsString())) return true;
        if (!Objects.equals(alunoExistente.asString("CEP"), jsonAluno.get("aluno_endereco_cep").getAsString())) return true;
        if (!Objects.equals(alunoExistente.asString("BAIRRO"), jsonAluno.get("aluno_endereco_bairro").getAsString())) return true;
        if (!Objects.equals(alunoExistente.asString("CIDADE"), jsonAluno.get("aluno_endereco_cidade").getAsString())) return true;
        if (!Objects.equals(alunoExistente.asString("UF"), jsonAluno.get("aluno_endereco_uf").getAsString())) return true;
        
        Date dataNascBanco = formatoBanco.parse(alunoExistente.asString("DATA_NASCIMENTO"));
        Date dataNascAPI = formatoAPI.parse(jsonAluno.get("aluno_data_nascimento").getAsString());
        if (!dataNascBanco.equals(dataNascAPI)) return true;
        
        if (!Objects.equals(alunoExistente.asString("TELEFONE_CELULAR"), jsonAluno.get("aluno_telefone_celular").getAsString())) return true;
        if (!Objects.equals(alunoExistente.asString("EMAIL"), jsonAluno.get("aluno_email").getAsString())) return true;
        
        return false;
    }

    private void updateAlunoCompleto(String alunoId, String endereco, String cep, String bairro, String cidade,
            String uf, String dataNascimento, String celular, String email, String situacaoId, String situacao,
            BigDecimal codEmp) throws Exception {
        
        JdbcWrapper jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        String sql = "UPDATE AD_ALUNOS SET " +
                     "ENDERECO = ?, " +
                     "CEP = ?, " +
                     "BAIRRO = ?, " +
                     "CIDADE = ?, " +
                     "UF = ?, " +
                     "DATA_NASCIMENTO = TO_DATE(?, 'YYYY-MM-DD'), " +
                     "TELEFONE_CELULAR = ?, " +
                     "EMAIL = ?, " +
                     "SITUACAO_ID = ?, " +
                     "SITUACAO = ?, " +
                     "DTALTER = SYSDATE " +
                     "WHERE ID_EXTERNO = ? AND CODEMP = ?";
        
        try (PreparedStatement pstmt = jdbc.getPreparedStatement(sql)) {
            pstmt.setString(1, endereco);
            pstmt.setString(2, cep);
            pstmt.setString(3, bairro);
            pstmt.setString(4, cidade);
            pstmt.setString(5, uf);
            pstmt.setString(6, dataNascimento);
            pstmt.setString(7, celular);
            pstmt.setString(8, email);
            pstmt.setString(9, situacaoId);
            pstmt.setString(10, situacao);
            pstmt.setString(11, alunoId);
            pstmt.setBigDecimal(12, codEmp);
            
            pstmt.executeUpdate();
        }
    }
}