package main

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/phpdave11/gofpdf"
)

type exportPayload struct {
	Name      string
	QueryText string
	Run       shared.RunResponse
	CreatedAt time.Time
}

func (app application) writeExport(w http.ResponseWriter, payload exportPayload, format string) {
	var (
		data        []byte
		contentType string
		filename    string
		err         error
	)

	switch format {
	case "pdf":
		data, err = buildPDF(payload)
		contentType = "application/pdf"
		filename = safeDownloadName(payload.Name, "pdf")
	case "docx":
		data, err = buildDOCX(payload)
		contentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
		filename = safeDownloadName(payload.Name, "docx")
	}

	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func buildPDF(payload exportPayload) ([]byte, error) {
	fontRegular, fontBold := findWindowsFonts()

	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetMargins(14, 16, 14)
	pdf.SetAutoPageBreak(true, 14)
	if fontRegular != "" {
		pdf.AddUTF8Font("ArialUnicode", "", fontRegular)
	}
	if fontBold != "" {
		pdf.AddUTF8Font("ArialUnicode", "B", fontBold)
	}
	fontFamily := "ArialUnicode"
	if fontRegular == "" {
		fontFamily = "Arial"
	}

	pdf.AddPage()
	pdf.SetFillColor(17, 35, 58)
	pdf.SetTextColor(255, 255, 255)
	pdf.RoundedRect(14, 16, 182, 28, 4, "1234", "F")
	pdf.SetFont(fontFamily, "B", 18)
	pdf.SetXY(20, 23)
	pdf.CellFormat(0, 8, payload.Name, "", 1, "L", false, 0, "")
	pdf.SetFont(fontFamily, "", 10)
	pdf.SetTextColor(220, 232, 244)
	pdf.SetX(20)
	pdf.CellFormat(0, 6, "Drivee Analytics • экспорт отчета", "", 1, "L", false, 0, "")

	pdf.SetTextColor(28, 36, 44)
	pdf.SetY(50)
	writePDFSection(pdf, fontFamily, "Запрос", payload.QueryText)
	writePDFSection(pdf, fontFamily, "Краткая интерпретация", payload.Run.Preview.Summary)
	writePDFSection(pdf, fontFamily, "SQL", payload.Run.SQL)

	pdf.Ln(2)
	pdf.SetFont(fontFamily, "B", 13)
	pdf.CellFormat(0, 8, "Ключевые показатели", "", 1, "L", false, 0, "")

	metrics := exportHighlights(payload)
	cardWidth := 57.0
	cardGap := 5.5
	startX := 14.0
	currentY := pdf.GetY()
	for idx, item := range metrics {
		x := startX + float64(idx)*(cardWidth+cardGap)
		pdf.SetXY(x, currentY)
		pdf.SetFillColor(242, 246, 251)
		pdf.SetDrawColor(219, 229, 239)
		pdf.RoundedRect(x, currentY, cardWidth, 22, 4, "1234", "FD")
		pdf.SetXY(x+4, currentY+4)
		pdf.SetFont(fontFamily, "", 9)
		pdf.SetTextColor(104, 121, 140)
		pdf.CellFormat(cardWidth-8, 4, item[0], "", 1, "L", false, 0, "")
		pdf.SetX(x + 4)
		pdf.SetFont(fontFamily, "B", 12)
		pdf.SetTextColor(28, 36, 44)
		pdf.MultiCell(cardWidth-8, 5, item[1], "", "L", false)
	}
	pdf.SetY(currentY + 28)

	pdf.SetFont(fontFamily, "B", 13)
	pdf.CellFormat(0, 8, "Табличный результат", "", 1, "L", false, 0, "")
	renderPDFTable(pdf, fontFamily, payload.Run.Result)

	var output bytes.Buffer
	if err := pdf.Output(&output); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func writePDFSection(pdf *gofpdf.Fpdf, fontFamily, title, text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	pdf.SetFont(fontFamily, "B", 12)
	pdf.SetTextColor(28, 36, 44)
	pdf.CellFormat(0, 7, title, "", 1, "L", false, 0, "")
	pdf.SetFont(fontFamily, "", 10)
	pdf.SetTextColor(76, 89, 103)
	pdf.MultiCell(0, 5, text, "", "L", false)
	pdf.Ln(2)
}

func renderPDFTable(pdf *gofpdf.Fpdf, fontFamily string, result shared.QueryResult) {
	if len(result.Columns) == 0 {
		pdf.SetFont(fontFamily, "", 10)
		pdf.MultiCell(0, 6, "Нет данных для отображения.", "", "L", false)
		return
	}

	columns := result.Columns
	if len(columns) > 4 {
		columns = columns[:4]
	}

	tableWidth := 182.0
	colWidth := tableWidth / float64(len(columns))

	pdf.SetFillColor(17, 35, 58)
	pdf.SetTextColor(255, 255, 255)
	pdf.SetFont(fontFamily, "B", 10)
	for _, column := range columns {
		pdf.CellFormat(colWidth, 8, column, "1", 0, "C", true, 0, "")
	}
	pdf.Ln(-1)

	pdf.SetFont(fontFamily, "", 9)
	pdf.SetTextColor(28, 36, 44)
	limit := len(result.Rows)
	if limit > 18 {
		limit = 18
	}
	for i := 0; i < limit; i++ {
		row := result.Rows[i]
		for columnIndex := range columns {
			value := ""
			if columnIndex < len(row) {
				value = row[columnIndex]
			}
			pdf.CellFormat(colWidth, 7, value, "1", 0, "L", false, 0, "")
		}
		pdf.Ln(-1)
	}
	if len(result.Rows) > limit {
		pdf.Ln(3)
		pdf.SetTextColor(104, 121, 140)
		pdf.CellFormat(0, 6, fmt.Sprintf("В экспорт включены первые %d строк из %d.", limit, len(result.Rows)), "", 1, "L", false, 0, "")
	}
}

func buildDOCX(payload exportPayload) ([]byte, error) {
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)

	files := map[string]string{
		"[Content_Types].xml": contentTypesXML(),
		"_rels/.rels":         rootRelsXML(),
		"word/document.xml":   documentXML(payload),
		"word/styles.xml":     stylesXML(),
	}

	for name, contents := range files {
		writer, err := archive.Create(name)
		if err != nil {
			return nil, err
		}
		if _, err := writer.Write([]byte(contents)); err != nil {
			return nil, err
		}
	}

	if err := archive.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func documentXML(payload exportPayload) string {
	var body strings.Builder
	writeDocParagraph(&body, payload.Name, "Title")
	writeDocParagraph(&body, "Drivee Analytics", "Subtitle")
	writeDocParagraph(&body, fmt.Sprintf("Сформировано: %s", payload.CreatedAt.Format("02.01.2006 15:04")), "")
	writeDocParagraph(&body, "Запрос", "Heading1")
	writeDocParagraph(&body, payload.QueryText, "")
	writeDocParagraph(&body, "Краткая интерпретация", "Heading1")
	writeDocParagraph(&body, payload.Run.Preview.Summary, "")
	writeDocParagraph(&body, "SQL", "Heading1")
	writeDocParagraph(&body, payload.Run.SQL, "Code")
	writeDocParagraph(&body, "Ключевые показатели", "Heading1")

	for _, item := range exportHighlights(payload) {
		writeDocParagraph(&body, fmt.Sprintf("%s: %s", item[0], item[1]), "")
	}

	writeDocParagraph(&body, "Табличный результат", "Heading1")
	body.WriteString(docTableXML(payload.Run.Result))

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:wpc="http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:wp14="http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing" xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" xmlns:w10="urn:schemas-microsoft-com:office:word" xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" mc:Ignorable="w14 wp14">
  <w:body>
    %s
    <w:sectPr>
      <w:pgSz w:w="11906" w:h="16838"/>
      <w:pgMar w:top="1000" w:right="900" w:bottom="1000" w:left="900" w:header="708" w:footer="708" w:gutter="0"/>
    </w:sectPr>
  </w:body>
</w:document>`, body.String())
}

func docTableXML(result shared.QueryResult) string {
	if len(result.Columns) == 0 {
		return ""
	}

	var builder strings.Builder
	builder.WriteString(`<w:tbl><w:tblPr><w:tblW w:w="0" w:type="auto"/><w:tblBorders><w:top w:val="single" w:sz="8" w:space="0" w:color="DCE2EA"/><w:left w:val="single" w:sz="8" w:space="0" w:color="DCE2EA"/><w:bottom w:val="single" w:sz="8" w:space="0" w:color="DCE2EA"/><w:right w:val="single" w:sz="8" w:space="0" w:color="DCE2EA"/><w:insideH w:val="single" w:sz="8" w:space="0" w:color="DCE2EA"/><w:insideV w:val="single" w:sz="8" w:space="0" w:color="DCE2EA"/></w:tblBorders></w:tblPr>`)
	builder.WriteString(`<w:tr>`)
	for _, column := range result.Columns {
		builder.WriteString(docTableCellXML(column, true))
	}
	builder.WriteString(`</w:tr>`)

	limit := len(result.Rows)
	if limit > 20 {
		limit = 20
	}
	for i := 0; i < limit; i++ {
		builder.WriteString(`<w:tr>`)
		for _, value := range result.Rows[i] {
			builder.WriteString(docTableCellXML(value, false))
		}
		builder.WriteString(`</w:tr>`)
	}
	builder.WriteString(`</w:tbl>`)
	return builder.String()
}

func docTableCellXML(text string, header bool) string {
	style := ""
	if header {
		style = `<w:shd w:val="clear" w:fill="11233A"/>`
	}
	color := ""
	if header {
		color = `<w:color w:val="FFFFFF"/>`
	}
	return fmt.Sprintf(`<w:tc><w:tcPr>%s</w:tcPr><w:p><w:r><w:rPr>%s</w:rPr><w:t xml:space="preserve">%s</w:t></w:r></w:p></w:tc>`, style, color, xmlEscape(text))
}

func writeDocParagraph(builder *strings.Builder, text, style string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	paragraphStyle := ""
	if style != "" {
		paragraphStyle = fmt.Sprintf(`<w:pPr><w:pStyle w:val="%s"/></w:pPr>`, style)
	}
	builder.WriteString(fmt.Sprintf(`<w:p>%s<w:r><w:t xml:space="preserve">%s</w:t></w:r></w:p>`, paragraphStyle, xmlEscape(text)))
}

func contentTypesXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
</Types>`
}

func rootRelsXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>`
}

func stylesXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:style w:type="paragraph" w:default="1" w:styleId="Normal">
    <w:name w:val="Normal"/>
    <w:rPr><w:sz w:val="22"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Title">
    <w:name w:val="Title"/>
    <w:rPr><w:b/><w:sz w:val="34"/><w:color w:val="11233A"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Subtitle">
    <w:name w:val="Subtitle"/>
    <w:rPr><w:sz w:val="22"/><w:color w:val="607486"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="Heading 1"/>
    <w:rPr><w:b/><w:sz w:val="28"/><w:color w:val="11233A"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Code">
    <w:name w:val="Code"/>
    <w:rPr><w:rFonts w:ascii="Consolas" w:hAnsi="Consolas"/><w:sz w:val="20"/><w:color w:val="33506A"/></w:rPr>
  </w:style>
</w:styles>`
}

func exportHighlights(payload exportPayload) [][2]string {
	result := payload.Run.Result
	rowCount := fmt.Sprintf("%d строк", result.Count)
	provider := payload.Run.Provider
	if provider == "" {
		provider = "rule-based"
	}
	topValue := "Нет данных"
	if len(result.Rows) > 0 {
		topValue = strings.Join(result.Rows[0], " • ")
	}
	return [][2]string{
		{"Источник интерпретации", provider},
		{"Снимок данных", rowCount},
		{"Первый результат", topValue},
	}
}

func safeDownloadName(name, extension string) string {
	value := strings.TrimSpace(name)
	if value == "" {
		value = "report"
	}
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]+`)
	value = re.ReplaceAllString(strings.ToLower(value), "-")
	value = strings.Trim(value, "-")
	if value == "" {
		value = "report"
	}
	return value + "." + extension
}

func findWindowsFonts() (string, string) {
	fontsDir := filepath.Join(os.Getenv("WINDIR"), "Fonts")
	if fontsDir == "" {
		fontsDir = `C:\Windows\Fonts`
	}
	regular := filepath.Join(fontsDir, "arial.ttf")
	bold := filepath.Join(fontsDir, "arialbd.ttf")
	if _, err := os.Stat(regular); err != nil {
		regular = ""
	}
	if _, err := os.Stat(bold); err != nil {
		bold = regular
	}
	return regular, bold
}

func xmlEscape(value string) string {
	var buffer bytes.Buffer
	_ = xml.EscapeText(&buffer, []byte(value))
	return buffer.String()
}
