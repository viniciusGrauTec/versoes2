package br.com.sankhya.acoesgrautec.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import br.com.sankhya.modelcore.MGEModelException;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRExporter;
import net.sf.jasperreports.engine.JRExporterParameter;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.data.JRXmlDataSource;
import net.sf.jasperreports.engine.export.JRPdfExporter;
import net.sf.jasperreports.engine.export.JRXlsExporter;

public class ReportHelperNfe {

	private static String gerarIdReport() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"ddMMyyyyhhmmss");
		String id = simpleDateFormat.format(new Date());

		return id;
	}

	/**
	 * Gera o relat�rio e retorna o nome do relat�rio gerado
	 * 
	 * @param nunota
	 * @param codEmp
	 * @throws MGEModelException 
	 * */
	public static String generateReportNfe(String reportFileName,
			String formatReport, Map<String, Object> params,
			JRXmlDataSource jrXmlDataSource, String vArqPdf, Long nunota) throws MGEModelException {
		try {
			if (params == null) {
				params = new HashMap<String, Object>();
			}
			if (formatReport == null) {
				formatReport = "PDF";
			}
			String reportFormated = reportFileName.endsWith(".jasper") ? reportFileName
					: (new StringBuilder()).append(reportFileName)
							.append(".jasper").toString();
			
			String sourceFileName = "/home/mgeweb/sankhyaW_gerenciador_de_pacotes/bin/report" + File.separator + reportFormated;
			
			net.sf.jasperreports.engine.JasperPrint jasperPrint = null;
			jasperPrint = JasperFillManager.fillReport(sourceFileName, params,
					jrXmlDataSource);
			
			StringBuilder nameReport = new StringBuilder();
			nameReport.append(reportFileName + "_" + gerarIdReport());
			JRExporter exporter = null;
			if (formatReport.equals("PDF")) {
				exporter = new JRPdfExporter();
				nameReport.append(".pdf");
			}
			File fileTarget = new File(vArqPdf);
			exporter.setParameter(JRExporterParameter.JASPER_PRINT, jasperPrint);
			exporter.setParameter(JRExporterParameter.OUTPUT_STREAM,
					new FileOutputStream(fileTarget.getAbsolutePath()));
			exporter.exportReport();

			return fileTarget.getPath();
		} catch (JRException e) {
			throw new MGEModelException("generic_error_executing_procedure" + e.toString());

		} catch (IOException e) {
			throw new MGEModelException("generic_error_executing_procedure" + e.toString());
		}
	}
}
