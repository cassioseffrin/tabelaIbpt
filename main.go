package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// ─── Config ──────────────────────────────────────────────────────────────────

const (
	dbHost         = "api.repag.com.br"
	dbPort         = 5432
	dbUser         = "postgres"
	dbPassword     = "Zeus@2540"
	dbName         = "tabela_ibpt"
	serverPort     = ":8090"
	permanentToken = "TWFycmVjb01hcmluZ2FQcmVzbzIwMjE"
	csvDir         = "/Users/cassioseffrin/Desktop/54836255000118"
)

// ─── Models ──────────────────────────────────────────────────────────────────

// CSVRecord represents a single row from the IBPT CSV file
type CSVRecord struct {
	Codigo            string
	Ex                string
	Tipo              string
	Descricao         string
	NacionalFederal   float64
	ImportadosFederal float64
	Estadual          float64
	Municipal         float64
	VigenciaInicio    string
	VigenciaFim       string
	Chave             string
	Versao            string
	Fonte             string
}

// IBPTProdutoResponse mirrors the exact response format of the official IBPT API
type IBPTProdutoResponse struct {
	Codigo                string  `json:"Codigo"`
	UF                    string  `json:"UF"`
	EX                    int     `json:"EX"`
	Descricao             string  `json:"Descricao"`
	Nacional              float64 `json:"Nacional"`
	Estadual              float64 `json:"Estadual"`
	Importado             float64 `json:"Importado"`
	Municipal             float64 `json:"Municipal"`
	Tipo                  string  `json:"Tipo"`
	VigenciaInicio        string  `json:"VigenciaInicio"`
	VigenciaFim           string  `json:"VigenciaFim"`
	Chave                 string  `json:"Chave"`
	Versao                string  `json:"Versao"`
	Fonte                 string  `json:"Fonte"`
	Valor                 float64 `json:"Valor"`
	ValorTributoNacional  float64 `json:"ValorTributoNacional"`
	ValorTributoEstadual  float64 `json:"ValorTributoEstadual"`
	ValorTributoImportado float64 `json:"ValorTributoImportado"`
	ValorTributoMunicipal float64 `json:"ValorTributoMunicipal"`
}

// ─── Database ────────────────────────────────────────────────────────────────

var (
	db *sql.DB
	mu sync.Mutex
)

func connectDB() *sql.DB {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=10",
		dbHost, dbPort, dbUser, dbPassword, dbName,
	)
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	if err := conn.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("✅ Connected to PostgreSQL")
	return conn
}

func ensureTables() {
	queries := []string{
		// Product table: federal data (same across all states)
		`CREATE TABLE IF NOT EXISTS ibpt_product (
			codigo           VARCHAR(20) NOT NULL,
			ex               VARCHAR(10) DEFAULT '',
			tipo             VARCHAR(10),
			descricao        TEXT,
			nacional_federal NUMERIC(10,2) DEFAULT 0,
			municipal        NUMERIC(10,2) DEFAULT 0,
			vigencia_inicio  VARCHAR(20),
			vigencia_fim     VARCHAR(20),
			chave            VARCHAR(20),
			versao           VARCHAR(20),
			fonte            VARCHAR(100),
			created_at       TIMESTAMP DEFAULT NOW(),
			PRIMARY KEY (codigo, ex)
		)`,
		// State tax table: state-specific data (ICMS + importados)
		`CREATE TABLE IF NOT EXISTS ibpt_state_tax (
			codigo             VARCHAR(20) NOT NULL,
			ex                 VARCHAR(10) DEFAULT '',
			uf                 VARCHAR(2) NOT NULL,
			estadual           NUMERIC(10,2) DEFAULT 0,
			importados_federal NUMERIC(10,2) DEFAULT 0,
			PRIMARY KEY (codigo, ex, uf)
		)`,
		// Indexes for fast lookups
		`CREATE INDEX IF NOT EXISTS idx_ibpt_product_codigo ON ibpt_product (codigo)`,
		`CREATE INDEX IF NOT EXISTS idx_ibpt_state_tax_codigo_uf ON ibpt_state_tax (codigo, uf)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			// Ignore "already exists" errors for indexes/tables
			if !strings.Contains(err.Error(), "already exists") {
				log.Printf("⚠️  SQL: %v", err)
			}
		}
	}
	log.Println("✅ Tables ensured (ibpt_product + ibpt_state_tax)")
}

// ─── CSV Parsing ─────────────────────────────────────────────────────────────

// extractUFFromFilename extracts UF (2-letter state code) from filenames like
// TabelaIBPTaxSC26.1.C.csv or CartazIBPTaxRS26.1.C.xlsx
func extractUFFromFilename(filename string) string {
	base := filepath.Base(filename)
	re := regexp.MustCompile(`(?i)IBPTax([A-Z]{2})`)
	matches := re.FindStringSubmatch(base)
	if len(matches) >= 2 {
		return strings.ToUpper(matches[1])
	}
	return ""
}

func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	s = strings.ReplaceAll(s, ",", ".")
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// readCSV reads a semicolon-separated IBPT CSV file
func readCSV(filePath string) ([]CSVRecord, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(transform.NewReader(f, charmap.Windows1252.NewDecoder()))
	// Increase buffer for long lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var records []CSVRecord
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip header
		if lineNum == 1 {
			continue
		}

		// Parse semicolon-separated line
		// Handle quoted fields (descricao may contain semicolons inside quotes)
		fields := parseCSVLine(line)
		if len(fields) < 13 {
			continue
		}

		rec := CSVRecord{
			Codigo:            strings.TrimSpace(fields[0]),
			Ex:                strings.TrimSpace(fields[1]),
			Tipo:              strings.TrimSpace(fields[2]),
			Descricao:         strings.Trim(strings.TrimSpace(fields[3]), "\""),
			NacionalFederal:   parseFloat(fields[4]),
			ImportadosFederal: parseFloat(fields[5]),
			Estadual:          parseFloat(fields[6]),
			Municipal:         parseFloat(fields[7]),
			VigenciaInicio:    strings.TrimSpace(fields[8]),
			VigenciaFim:       strings.TrimSpace(fields[9]),
			Chave:             strings.TrimSpace(fields[10]),
			Versao:            strings.TrimSpace(fields[11]),
			Fonte:             strings.TrimSpace(fields[12]),
		}
		if rec.Codigo != "" {
			records = append(records, rec)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return records, nil
}

// parseCSVLine splits a semicolon-separated line respecting quoted fields
func parseCSVLine(line string) []string {
	var fields []string
	var current strings.Builder
	inQuotes := false

	for i := 0; i < len(line); i++ {
		ch := line[i]
		if ch == '"' {
			inQuotes = !inQuotes
			current.WriteByte(ch)
		} else if ch == ';' && !inQuotes {
			fields = append(fields, current.String())
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	fields = append(fields, current.String())
	return fields
}

// ─── Bulk Insert ─────────────────────────────────────────────────────────────

func bulkInsertProducts(tx *sql.Tx, records []CSVRecord) (int, error) {
	stmt, err := tx.Prepare(pq.CopyIn("ibpt_product",
		"codigo", "ex", "tipo", "descricao",
		"nacional_federal", "municipal",
		"vigencia_inicio", "vigencia_fim", "chave", "versao", "fonte",
	))
	if err != nil {
		return 0, fmt.Errorf("prepare COPY ibpt_product: %w", err)
	}

	// Track unique (codigo, ex) to avoid duplicates
	seen := make(map[string]bool, len(records))
	count := 0

	for _, r := range records {
		key := r.Codigo + "|" + r.Ex
		if seen[key] {
			continue
		}
		seen[key] = true

		if _, err := stmt.Exec(
			r.Codigo, r.Ex, r.Tipo, r.Descricao,
			r.NacionalFederal, r.Municipal,
			r.VigenciaInicio, r.VigenciaFim, r.Chave, r.Versao, r.Fonte,
		); err != nil {
			return 0, fmt.Errorf("copy product exec: %w", err)
		}
		count++
	}

	if _, err := stmt.Exec(); err != nil {
		return 0, fmt.Errorf("copy product flush: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return 0, fmt.Errorf("close product statement: %w", err)
	}
	return count, nil
}

func bulkInsertStateTaxes(tx *sql.Tx, records []CSVRecord, uf string) (int, error) {
	stmt, err := tx.Prepare(pq.CopyIn("ibpt_state_tax",
		"codigo", "ex", "uf", "estadual", "importados_federal",
	))
	if err != nil {
		return 0, fmt.Errorf("prepare COPY ibpt_state_tax: %w", err)
	}

	for _, r := range records {
		if _, err := stmt.Exec(
			r.Codigo, r.Ex, uf, r.Estadual, r.ImportadosFederal,
		); err != nil {
			return 0, fmt.Errorf("copy state_tax exec: %w", err)
		}
	}

	if _, err := stmt.Exec(); err != nil {
		return 0, fmt.Errorf("copy state_tax flush: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return 0, fmt.Errorf("close state_tax statement: %w", err)
	}
	return len(records), nil
}

// ─── Process CSV files ──────────────────────────────────────────────────────

func processAllCSVFiles(dir string) error {
	mu.Lock()
	defer mu.Unlock()

	start := time.Now()

	// Find all CSV files matching pattern
	pattern := filepath.Join(dir, "TabelaIBPTax*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no CSV files found matching %s", pattern)
	}

	log.Printf("📂 Found %d CSV files to process", len(files))

	// Begin transaction
	log.Println("📦 Starting transaction...")
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Drop and recreate tables for clean reload (avoids PK/lock issues)
	log.Println("🗑️  Dropping existing data...")
	tx.Exec("DROP TABLE IF EXISTS ibpt_state_tax")
	tx.Exec("DROP TABLE IF EXISTS ibpt_product")

	log.Println("📐 Creating tables...")
	tx.Exec(`CREATE TABLE ibpt_product (
		codigo           VARCHAR(20) NOT NULL,
		ex               VARCHAR(10) DEFAULT '',
		tipo             VARCHAR(10),
		descricao        TEXT,
		nacional_federal NUMERIC(10,2) DEFAULT 0,
		municipal        NUMERIC(10,2) DEFAULT 0,
		vigencia_inicio  VARCHAR(20),
		vigencia_fim     VARCHAR(20),
		chave            VARCHAR(20),
		versao           VARCHAR(20),
		fonte            VARCHAR(100),
		created_at       TIMESTAMP DEFAULT NOW(),
		PRIMARY KEY (codigo, ex)
	)`)
	tx.Exec(`CREATE TABLE ibpt_state_tax (
		codigo             VARCHAR(20) NOT NULL,
		ex                 VARCHAR(10) DEFAULT '',
		uf                 VARCHAR(2) NOT NULL,
		estadual           NUMERIC(10,2) DEFAULT 0,
		importados_federal NUMERIC(10,2) DEFAULT 0,
		PRIMARY KEY (codigo, ex, uf)
	)`)

	// Process first file for products (federal data)
	log.Printf("📄 Reading CSV: %s ...", filepath.Base(files[0]))
	firstRecords, err := readCSV(files[0])
	if err != nil {
		return fmt.Errorf("read first CSV: %w", err)
	}
	log.Printf("📄 Read %d records, inserting products...", len(firstRecords))
	productCount, err := bulkInsertProducts(tx, firstRecords)
	if err != nil {
		return fmt.Errorf("insert products: %w", err)
	}
	log.Printf("📄 Inserted %d products (federal data) from %s", productCount, filepath.Base(files[0]))

	// Process all files for state taxes
	totalStateTaxes := 0
	for _, file := range files {
		uf := extractUFFromFilename(file)
		if uf == "" {
			log.Printf("⚠️  Could not extract UF from filename: %s, skipping", file)
			continue
		}

		records, err := readCSV(file)
		if err != nil {
			log.Printf("⚠️  Error reading %s: %v", file, err)
			continue
		}

		count, err := bulkInsertStateTaxes(tx, records, uf)
		if err != nil {
			return fmt.Errorf("insert state taxes for %s: %w", uf, err)
		}
		totalStateTaxes += count
		log.Printf("   📄 %s: %d state tax records", uf, count)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	elapsed := time.Since(start)
	log.Printf("✅ Import complete: %d products + %d state taxes in %v",
		productCount, totalStateTaxes, elapsed.Round(time.Millisecond))
	return nil
}

func processSingleCSV(filePath string) (string, int, error) {
	mu.Lock()
	defer mu.Unlock()

	uf := extractUFFromFilename(filePath)
	if uf == "" {
		return "", 0, fmt.Errorf("could not extract UF from filename: %s", filepath.Base(filePath))
	}

	records, err := readCSV(filePath)
	if err != nil {
		return uf, 0, err
	}

	tx, err := db.Begin()
	if err != nil {
		return uf, 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Upsert: delete existing state data for this UF, then re-insert
	if _, err := tx.Exec("DELETE FROM ibpt_state_tax WHERE uf = $1", uf); err != nil {
		return uf, 0, fmt.Errorf("delete state tax for %s: %w", uf, err)
	}

	// Also update product data (federal)
	if _, err := tx.Exec("TRUNCATE TABLE ibpt_product"); err != nil {
		return uf, 0, fmt.Errorf("truncate ibpt_product: %w", err)
	}

	productCount, err := bulkInsertProducts(tx, records)
	if err != nil {
		return uf, 0, fmt.Errorf("insert products: %w", err)
	}

	stateCount, err := bulkInsertStateTaxes(tx, records, uf)
	if err != nil {
		return uf, 0, fmt.Errorf("insert state taxes: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return uf, 0, fmt.Errorf("commit: %w", err)
	}

	log.Printf("✅ Processed %s: %d products + %d state taxes", uf, productCount, stateCount)
	return uf, stateCount, nil
}

// ─── HTTP Handlers ───────────────────────────────────────────────────────────

func handleUploadPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, uploadHTML)
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := r.ParseMultipartForm(100 << 20); err != nil { // 100MB max
		jsonError(w, "Erro ao fazer parse do upload: "+err.Error(), http.StatusBadRequest)
		return
	}

	files := r.MultipartForm.File["files"]
	if len(files) == 0 {
		// Try single file field
		file, header, err := r.FormFile("file")
		if err != nil {
			jsonError(w, "Nenhum arquivo encontrado no upload", http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Save single file
		dstPath := filepath.Join(csvDir, header.Filename)
		dst, err := os.Create(dstPath)
		if err != nil {
			jsonError(w, "Erro ao salvar arquivo: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := io.Copy(dst, file); err != nil {
			dst.Close()
			jsonError(w, "Erro ao copiar arquivo: "+err.Error(), http.StatusInternalServerError)
			return
		}
		dst.Close()

		uf, count, err := processSingleCSV(dstPath)
		if err != nil {
			jsonError(w, "Erro ao processar: "+err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("UF %s: %d registros processados", uf, count),
			"uf":      uf,
			"records": count,
		})
		return
	}

	// Multiple files
	results := []map[string]interface{}{}
	for _, fh := range files {
		file, err := fh.Open()
		if err != nil {
			results = append(results, map[string]interface{}{"file": fh.Filename, "error": err.Error()})
			continue
		}

		dstPath := filepath.Join(csvDir, fh.Filename)
		dst, err := os.Create(dstPath)
		if err != nil {
			file.Close()
			results = append(results, map[string]interface{}{"file": fh.Filename, "error": err.Error()})
			continue
		}
		io.Copy(dst, file)
		dst.Close()
		file.Close()
	}

	// Reprocess all
	if err := processAllCSVFiles(csvDir); err != nil {
		jsonError(w, "Erro ao processar: "+err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Todos os arquivos processados com sucesso",
		"files":   results,
	})
}

func handleReprocessAll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	start := time.Now()
	if err := processAllCSVFiles(csvDir); err != nil {
		jsonError(w, "Erro ao reprocessar: "+err.Error(), http.StatusInternalServerError)
		return
	}
	elapsed := time.Since(start)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":  true,
		"message":  fmt.Sprintf("Reprocessado em %v", elapsed.Round(time.Millisecond)),
		"duration": elapsed.String(),
	})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var productCount int
	db.QueryRow("SELECT COUNT(*) FROM ibpt_product").Scan(&productCount)

	var stateCount int
	db.QueryRow("SELECT COUNT(*) FROM ibpt_state_tax").Scan(&stateCount)

	// Get list of UFs
	rows, _ := db.Query("SELECT DISTINCT uf FROM ibpt_state_tax ORDER BY uf")
	var ufs []string
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var uf string
			rows.Scan(&uf)
			ufs = append(ufs, uf)
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"products":    productCount,
		"state_taxes": stateCount,
		"ufs":         ufs,
	})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	codigo := r.URL.Query().Get("codigo")
	if codigo == "" {
		jsonError(w, "Parâmetro 'codigo' é obrigatório", http.StatusBadRequest)
		return
	}
	uf := strings.ToUpper(r.URL.Query().Get("uf"))

	var query string
	var args []interface{}

	if uf != "" {
		query = `
			SELECT p.codigo, p.ex, p.tipo, p.descricao,
			       p.nacional_federal, s.importados_federal, s.estadual, p.municipal,
			       p.vigencia_inicio, p.vigencia_fim, p.chave, p.versao, p.fonte, s.uf
			FROM ibpt_product p
			JOIN ibpt_state_tax s ON s.codigo = p.codigo AND s.ex = p.ex
			WHERE p.codigo = $1 AND s.uf = $2
		`
		args = []interface{}{codigo, uf}
	} else {
		query = `
			SELECT p.codigo, p.ex, p.tipo, p.descricao,
			       p.nacional_federal, s.importados_federal, s.estadual, p.municipal,
			       p.vigencia_inicio, p.vigencia_fim, p.chave, p.versao, p.fonte, s.uf
			FROM ibpt_product p
			JOIN ibpt_state_tax s ON s.codigo = p.codigo AND s.ex = p.ex
			WHERE p.codigo = $1
			ORDER BY s.uf
		`
		args = []interface{}{codigo}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		jsonError(w, "Erro na consulta: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var codigo, ex, tipo, descricao, vigInicio, vigFim, chave, versao, fonte, ufResult string
		var nacional, importados, estadual, municipal float64
		if err := rows.Scan(
			&codigo, &ex, &tipo, &descricao,
			&nacional, &importados, &estadual, &municipal,
			&vigInicio, &vigFim, &chave, &versao, &fonte, &ufResult,
		); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"codigo":     codigo,
			"ex":         ex,
			"tipo":       tipo,
			"descricao":  descricao,
			"uf":         ufResult,
			"nacional":   nacional,
			"importados": importados,
			"estadual":   estadual,
			"municipal":  municipal,
			"vigencia":   vigInicio + " - " + vigFim,
			"versao":     versao,
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"results": results,
		"count":   len(results),
	})
}

// ─── IBPT API Endpoint (drop-in replacement for apidoni.ibpt.org.br) ─────────

func handleBuscarTributo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// ── Auth ──
	token := r.URL.Query().Get("token")
	if token == "" {
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		}
	}
	if token != permanentToken {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "Token inválido ou ausente",
		})
		return
	}

	// ── Params ──
	codigo := r.URL.Query().Get("codigo")
	if codigo == "" {
		jsonError(w, "Parâmetro 'codigo' (NCM) é obrigatório", http.StatusBadRequest)
		return
	}
	ex := r.URL.Query().Get("ex")
	if ex == "" {
		ex = ""
	}
	uf := strings.ToUpper(r.URL.Query().Get("uf"))
	if uf == "" {
		uf = "SC"
	}
	valorStr := r.URL.Query().Get("valor")
	valor := 0.0
	if valorStr != "" {
		v, err := strconv.ParseFloat(valorStr, 64)
		if err == nil {
			valor = v
		}
	}
	descricao := r.URL.Query().Get("descricao")

	// ── Query with JOIN ──
	query := `
		SELECT p.codigo, p.ex, p.tipo, p.descricao,
		       p.nacional_federal, s.importados_federal, s.estadual, p.municipal,
		       p.vigencia_inicio, p.vigencia_fim, p.chave, p.versao, p.fonte
		FROM ibpt_product p
		JOIN ibpt_state_tax s ON s.codigo = p.codigo AND s.ex = p.ex
		WHERE p.codigo = $1 AND p.ex = $2 AND s.uf = $3
		LIMIT 1
	`

	var rec struct {
		Codigo, Ex, Tipo, Descricao               string
		Nacional, Importados, Estadual, Municipal float64
		VigInicio, VigFim, Chave, Versao, Fonte   string
	}

	err := db.QueryRow(query, codigo, ex, uf).Scan(
		&rec.Codigo, &rec.Ex, &rec.Tipo, &rec.Descricao,
		&rec.Nacional, &rec.Importados, &rec.Estadual, &rec.Municipal,
		&rec.VigInicio, &rec.VigFim, &rec.Chave, &rec.Versao, &rec.Fonte,
	)
	if err == sql.ErrNoRows {
		// Fallback: try without ex filter
		query2 := `
			SELECT p.codigo, p.ex, p.tipo, p.descricao,
			       p.nacional_federal, s.importados_federal, s.estadual, p.municipal,
			       p.vigencia_inicio, p.vigencia_fim, p.chave, p.versao, p.fonte
			FROM ibpt_product p
			JOIN ibpt_state_tax s ON s.codigo = p.codigo AND s.ex = p.ex
			WHERE p.codigo = $1 AND s.uf = $2
			LIMIT 1
		`
		err = db.QueryRow(query2, codigo, uf).Scan(
			&rec.Codigo, &rec.Ex, &rec.Tipo, &rec.Descricao,
			&rec.Nacional, &rec.Importados, &rec.Estadual, &rec.Municipal,
			&rec.VigInicio, &rec.VigFim, &rec.Chave, &rec.Versao, &rec.Fonte,
		)
	}
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": fmt.Sprintf("NCM %s (ex=%s, uf=%s) não encontrado", codigo, ex, uf),
		})
		return
	}
	if err != nil {
		jsonError(w, "Erro ao consultar banco: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// ── Calculate tax values ──
	valorTribNacional := roundTo2(valor * rec.Nacional / 100.0)
	valorTribEstadual := roundTo2(valor * rec.Estadual / 100.0)
	valorTribMunicipal := roundTo2(valor * rec.Municipal / 100.0)
	valorTribImportado := roundTo2(valor * rec.Importados / 100.0)

	if descricao == "" {
		descricao = rec.Descricao
	}

	exInt, _ := strconv.Atoi(rec.Ex)

	resp := IBPTProdutoResponse{
		Codigo:                rec.Codigo,
		UF:                    uf,
		EX:                    exInt,
		Descricao:             descricao,
		Nacional:              rec.Nacional,
		Estadual:              rec.Estadual,
		Importado:             rec.Importados,
		Municipal:             rec.Municipal,
		Tipo:                  rec.Tipo,
		VigenciaInicio:        rec.VigInicio,
		VigenciaFim:           rec.VigFim,
		Chave:                 rec.Chave,
		Versao:                rec.Versao,
		Fonte:                 rec.Fonte,
		Valor:                 valor,
		ValorTributoNacional:  valorTribNacional,
		ValorTributoEstadual:  valorTribEstadual,
		ValorTributoImportado: valorTribImportado,
		ValorTributoMunicipal: valorTribMunicipal,
	}

	log.Printf("🔍 NCM %s (ex=%s, uf=%s, valor=%.2f): Fed=%.2f%% Est=%.2f%% Mun=%.2f%%",
		codigo, ex, uf, valor, rec.Nacional, rec.Estadual, rec.Municipal)

	json.NewEncoder(w).Encode(resp)
}

func roundTo2(v float64) float64 {
	return math.Round(v*100) / 100
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   msg,
	})
}

// ─── HTML Template ───────────────────────────────────────────────────────────

const uploadHTML = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IBPT Tax Manager</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e1a;
            --bg-secondary: #111827;
            --bg-card: #1a1f35;
            --bg-card-hover: #222845;
            --accent-blue: #3b82f6;
            --accent-purple: #8b5cf6;
            --accent-green: #10b981;
            --accent-red: #ef4444;
            --accent-amber: #f59e0b;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --border: #2a2f45;
            --gradient-1: linear-gradient(135deg, #3b82f6, #8b5cf6);
            --gradient-2: linear-gradient(135deg, #10b981, #3b82f6);
            --shadow-lg: 0 20px 60px rgba(0,0,0,0.4);
            --shadow-glow: 0 0 40px rgba(59,130,246,0.15);
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            overflow-y: auto;
        }
        body::before {
            content: '';
            position: fixed;
            top: -50%; left: -50%;
            width: 200%; height: 200%;
            background: radial-gradient(circle at 30% 30%, rgba(59,130,246,0.05) 0%, transparent 50%),
                        radial-gradient(circle at 70% 70%, rgba(139,92,246,0.05) 0%, transparent 50%);
            animation: bgShift 20s ease-in-out infinite alternate;
            z-index: 0;
        }
        @keyframes bgShift {
            0% { transform: translate(0, 0); }
            100% { transform: translate(-5%, -5%); }
        }
        .container {
            position: relative; z-index: 1;
            max-width: 620px; margin: 40px auto; padding: 20px;
        }
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 36px;
            box-shadow: var(--shadow-lg), var(--shadow-glow);
        }
        .header { text-align: center; margin-bottom: 28px; }
        .header .icon {
            width: 64px; height: 64px;
            background: var(--gradient-1);
            border-radius: 16px;
            display: inline-flex; align-items: center; justify-content: center;
            font-size: 28px; margin-bottom: 14px;
            box-shadow: 0 8px 32px rgba(59,130,246,0.3);
        }
        .header h1 {
            font-size: 24px; font-weight: 700;
            background: var(--gradient-1);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }
        .header p { color: var(--text-secondary); font-size: 14px; margin-top: 4px; }
        .stats-bar { display: flex; gap: 10px; margin-bottom: 24px; flex-wrap: wrap; }
        .stat {
            flex: 1; min-width: 100px;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 12px; padding: 12px; text-align: center;
            transition: border-color 0.3s;
        }
        .stat:hover { border-color: var(--accent-blue); }
        .stat .value { font-size: 20px; font-weight: 700; color: var(--accent-blue); }
        .stat .label { font-size: 10px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }
        .uf-badges { margin-bottom: 20px; display: flex; gap: 6px; flex-wrap: wrap; justify-content: center; }
        .uf-badge {
            background: var(--bg-secondary); border: 1px solid var(--border);
            border-radius: 8px; padding: 4px 10px;
            font-size: 12px; font-weight: 600; color: var(--accent-green);
            transition: all 0.2s;
        }
        .uf-badge:hover { border-color: var(--accent-green); }
        .drop-zone {
            border: 2px dashed var(--border);
            border-radius: 16px; padding: 36px 20px;
            text-align: center; cursor: pointer;
            transition: all 0.3s ease; position: relative;
            margin-bottom: 16px;
        }
        .drop-zone:hover, .drop-zone.dragover {
            border-color: var(--accent-blue);
            background: rgba(59,130,246,0.05);
        }
        .drop-zone .upload-icon { font-size: 36px; margin-bottom: 10px; display: block; }
        .drop-zone .text-main { font-size: 14px; font-weight: 500; }
        .drop-zone .text-sub { font-size: 12px; color: var(--text-muted); margin-top: 4px; }
        .drop-zone input[type="file"] {
            position: absolute; top: 0; left: 0; width: 100%; height: 100%; opacity: 0; cursor: pointer;
        }
        .file-list { margin-bottom: 16px; }
        .file-item {
            display: flex; align-items: center; gap: 10px;
            background: var(--bg-secondary); border: 1px solid var(--border);
            border-radius: 10px; padding: 10px 14px; margin-bottom: 6px;
            font-size: 13px; animation: fadeIn 0.3s;
        }
        .file-item .uf-tag {
            background: var(--gradient-1); color: white;
            padding: 2px 8px; border-radius: 6px; font-weight: 600; font-size: 11px;
        }
        .file-item .fname { flex: 1; color: var(--text-secondary); }
        .file-item .fsize { color: var(--text-muted); font-size: 11px; }
        .file-item .remove { background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 16px; padding: 2px; border-radius: 4px; }
        .file-item .remove:hover { color: var(--accent-red); background: rgba(239,68,68,0.1); }
        .btn {
            width: 100%; padding: 13px; border: none; border-radius: 12px;
            background: var(--gradient-1); color: white;
            font-family: 'Inter', sans-serif; font-size: 14px; font-weight: 600;
            cursor: pointer; transition: all 0.3s; margin-bottom: 8px;
        }
        .btn:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 8px 30px rgba(59,130,246,0.4); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .btn-secondary { background: var(--bg-card-hover); border: 1px solid var(--border); }
        .btn-secondary:hover:not(:disabled) { box-shadow: none; border-color: var(--accent-blue); }
        .result {
            display: none; margin-top: 16px; padding: 14px; border-radius: 12px;
            font-size: 13px; line-height: 1.5; animation: fadeIn 0.4s;
        }
        .result.active { display: block; }
        .result.success { background: rgba(16,185,129,0.1); border: 1px solid rgba(16,185,129,0.3); color: var(--accent-green); }
        .result.error { background: rgba(239,68,68,0.1); border: 1px solid rgba(239,68,68,0.3); color: var(--accent-red); }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
        .search-section { margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--border); }
        .search-section h3 { font-size: 14px; font-weight: 600; margin-bottom: 10px; }
        .search-row { display: flex; gap: 8px; margin-bottom: 10px; }
        .search-row input, .search-row select {
            padding: 9px 12px; background: var(--bg-secondary); border: 1px solid var(--border);
            border-radius: 10px; color: var(--text-primary); font-family: 'Inter', sans-serif;
            font-size: 13px; outline: none; transition: border-color 0.3s;
        }
        .search-row input:focus, .search-row select:focus { border-color: var(--accent-blue); }
        .search-row input { flex: 1; }
        .search-row select { width: 80px; }
        .search-row input::placeholder { color: var(--text-muted); }
        .search-row button {
            padding: 9px 18px; background: var(--bg-card-hover); border: 1px solid var(--border);
            border-radius: 10px; color: var(--text-primary); font-family: 'Inter', sans-serif;
            font-size: 13px; font-weight: 500; cursor: pointer; transition: all 0.2s;
        }
        .search-row button:hover { background: var(--accent-blue); border-color: var(--accent-blue); }
        .search-results { margin-top: 10px; max-height: 300px; overflow-y: auto; }
        .search-results::-webkit-scrollbar { width: 4px; }
        .search-results::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
        .search-item {
            background: var(--bg-secondary); border: 1px solid var(--border);
            border-radius: 10px; padding: 12px 14px; margin-bottom: 6px;
            font-size: 12px; animation: fadeIn 0.3s;
        }
        .search-item .top { display: flex; justify-content: space-between; align-items: center; }
        .search-item .code { font-weight: 600; color: var(--accent-blue); font-size: 13px; }
        .search-item .uf-label { background: var(--gradient-1); color: white; padding: 2px 8px; border-radius: 6px; font-weight: 600; font-size: 10px; }
        .search-item .desc { color: var(--text-secondary); margin: 4px 0; }
        .search-item .taxes { display: flex; gap: 14px; margin-top: 6px; color: var(--text-muted); }
        .search-item .taxes span { color: var(--accent-green); font-weight: 500; }
        .spinner {
            display: inline-block; width: 16px; height: 16px;
            border: 2px solid transparent; border-top: 2px solid white;
            border-radius: 50%; animation: spin 0.7s linear infinite;
            vertical-align: middle; margin-right: 6px;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <div class="header">
                <div class="icon">📊</div>
                <h1>IBPT Tax Manager</h1>
                <p>Importação multi-UF com CSV • Lei 12.741</p>
            </div>

            <div class="stats-bar">
                <div class="stat">
                    <div class="value" id="productCount">—</div>
                    <div class="label">Produtos</div>
                </div>
                <div class="stat">
                    <div class="value" id="stateCount">—</div>
                    <div class="label">Impostos UF</div>
                </div>
                <div class="stat">
                    <div class="value" id="ufCount">—</div>
                    <div class="label">Estados</div>
                </div>
            </div>
            <div class="uf-badges" id="ufBadges"></div>

            <div class="drop-zone" id="dropZone">
                <span class="upload-icon">📁</span>
                <div class="text-main">Arraste arquivos CSV aqui</div>
                <div class="text-sub">TabelaIBPTax{UF}*.csv • Múltiplos arquivos aceitos</div>
                <input type="file" id="fileInput" accept=".csv" multiple>
            </div>

            <div class="file-list" id="fileList"></div>

            <button class="btn" id="uploadBtn" disabled>Enviar e Processar</button>
            <button class="btn btn-secondary" id="reprocessBtn">🔄 Reprocessar todos do servidor</button>

            <div class="result" id="result"></div>

            <div class="search-section">
                <h3>🔍 Consultar NCM por UF</h3>
                <div class="search-row">
                    <input type="text" id="searchInput" placeholder="NCM (ex: 71171900)">
                    <select id="ufSelect"><option value="">UF</option></select>
                    <button id="searchBtn">Buscar</button>
                </div>
                <div class="search-results" id="searchResults"></div>
            </div>
        </div>
    </div>

    <script>
        const dropZone = document.getElementById('dropZone');
        const fileInput = document.getElementById('fileInput');
        const fileList = document.getElementById('fileList');
        const uploadBtn = document.getElementById('uploadBtn');
        const reprocessBtn = document.getElementById('reprocessBtn');
        const result = document.getElementById('result');
        const ufSelect = document.getElementById('ufSelect');
        let selectedFiles = [];

        // Load stats
        function loadStats() {
            fetch('/api/status').then(r => r.json()).then(d => {
                document.getElementById('productCount').textContent = d.products?.toLocaleString('pt-BR') || '0';
                document.getElementById('stateCount').textContent = d.state_taxes?.toLocaleString('pt-BR') || '0';
                document.getElementById('ufCount').textContent = d.ufs?.length || '0';
                const badges = document.getElementById('ufBadges');
                badges.innerHTML = (d.ufs || []).map(u => '<span class="uf-badge">' + u + '</span>').join('');
                // Populate UF select
                const existingOpts = new Set([...ufSelect.options].map(o => o.value));
                (d.ufs || []).forEach(u => {
                    if (!existingOpts.has(u)) {
                        const opt = document.createElement('option');
                        opt.value = u; opt.textContent = u;
                        ufSelect.appendChild(opt);
                    }
                });
            }).catch(() => {});
        }
        loadStats();

        // File handling
        const ufPattern = /IBPTax([A-Z]{2})/i;
        function extractUF(name) { const m = name.match(ufPattern); return m ? m[1].toUpperCase() : '??'; }
        function formatBytes(b) {
            if (!b) return '0B';
            const i = Math.floor(Math.log(b) / Math.log(1024));
            return (b / Math.pow(1024, i)).toFixed(1) + ' ' + ['B','KB','MB'][i];
        }

        ['dragenter','dragover'].forEach(e => dropZone.addEventListener(e, ev => { ev.preventDefault(); dropZone.classList.add('dragover'); }));
        ['dragleave','drop'].forEach(e => dropZone.addEventListener(e, ev => { ev.preventDefault(); dropZone.classList.remove('dragover'); }));
        dropZone.addEventListener('drop', e => { addFiles(e.dataTransfer.files); });
        fileInput.addEventListener('change', () => { addFiles(fileInput.files); fileInput.value = ''; });

        function addFiles(fileListObj) {
            for (const f of fileListObj) {
                if (!selectedFiles.find(sf => sf.name === f.name)) selectedFiles.push(f);
            }
            renderFileList();
        }
        function removeFile(idx) { selectedFiles.splice(idx, 1); renderFileList(); }
        function renderFileList() {
            fileList.innerHTML = selectedFiles.map((f, i) =>
                '<div class="file-item"><span class="uf-tag">' + extractUF(f.name) + '</span>' +
                '<span class="fname">' + f.name + '</span>' +
                '<span class="fsize">' + formatBytes(f.size) + '</span>' +
                '<button class="remove" onclick="removeFile(' + i + ')">✕</button></div>'
            ).join('');
            uploadBtn.disabled = selectedFiles.length === 0;
        }

        uploadBtn.addEventListener('click', async () => {
            if (!selectedFiles.length) return;
            uploadBtn.disabled = true;
            uploadBtn.innerHTML = '<span class="spinner"></span> Processando...';
            result.classList.remove('active');

            const formData = new FormData();
            selectedFiles.forEach(f => formData.append('files', f));

            try {
                const res = await fetch('/api/upload', { method: 'POST', body: formData });
                const data = await res.json();
                if (data.success) {
                    result.className = 'result active success';
                    result.innerHTML = '✅ ' + data.message;
                    selectedFiles = []; renderFileList(); loadStats();
                } else {
                    result.className = 'result active error';
                    result.innerHTML = '❌ ' + (data.error || 'Erro');
                }
            } catch (err) {
                result.className = 'result active error';
                result.innerHTML = '❌ ' + err.message;
            }
            uploadBtn.innerHTML = 'Enviar e Processar';
            uploadBtn.disabled = selectedFiles.length === 0;
        });

        reprocessBtn.addEventListener('click', async () => {
            reprocessBtn.disabled = true;
            reprocessBtn.innerHTML = '<span class="spinner" style="border-top-color:var(--accent-blue)"></span> Reprocessando...';
            result.classList.remove('active');
            try {
                const res = await fetch('/api/reprocess', { method: 'POST' });
                const data = await res.json();
                if (data.success) {
                    result.className = 'result active success';
                    result.innerHTML = '✅ ' + data.message;
                    loadStats();
                } else {
                    result.className = 'result active error';
                    result.innerHTML = '❌ ' + (data.error || 'Erro');
                }
            } catch (err) {
                result.className = 'result active error';
                result.innerHTML = '❌ ' + err.message;
            }
            reprocessBtn.innerHTML = '🔄 Reprocessar todos do servidor';
            reprocessBtn.disabled = false;
        });

        // Search
        const searchBtn = document.getElementById('searchBtn');
        const searchInput = document.getElementById('searchInput');
        const searchResults = document.getElementById('searchResults');
        searchBtn.addEventListener('click', doSearch);
        searchInput.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });

        async function doSearch() {
            const q = searchInput.value.trim();
            if (!q) return;
            const uf = ufSelect.value;
            searchResults.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:14px;"><span class="spinner" style="border-top-color:var(--accent-blue)"></span> Buscando...</div>';
            try {
                let url = '/api/search?codigo=' + encodeURIComponent(q);
                if (uf) url += '&uf=' + uf;
                const res = await fetch(url);
                const data = await res.json();
                if (!data.results || data.results.length === 0) {
                    searchResults.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:14px;">Nenhum resultado</div>';
                    return;
                }
                searchResults.innerHTML = data.results.map(r =>
                    '<div class="search-item">' +
                    '<div class="top"><span class="code">' + r.codigo + (r.ex ? ' (ex:' + r.ex + ')' : '') + '</span>' +
                    '<span class="uf-label">' + r.uf + '</span></div>' +
                    '<div class="desc">' + r.descricao + '</div>' +
                    '<div class="taxes">' +
                    'Nacional: <span>' + r.nacional + '%</span> | ' +
                    'Importados: <span>' + r.importados + '%</span> | ' +
                    'Estadual: <span>' + r.estadual + '%</span> | ' +
                    'Municipal: <span>' + r.municipal + '%</span></div>' +
                    '</div>'
                ).join('');
            } catch (err) {
                searchResults.innerHTML = '<div style="text-align:center;color:var(--accent-red);padding:14px;">' + err.message + '</div>';
            }
        }
    </script>
</body>
</html>`

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("🚀 Starting IBPT Tax Manager (multi-UF)...")

	db = connectDB()
	defer db.Close()

	ensureTables()

	// Auto-import CSV files on startup
	if err := processAllCSVFiles(csvDir); err != nil {
		log.Printf("⚠️  Initial CSV import: %v", err)
	}

	// Routes
	r := mux.NewRouter()
	r.HandleFunc("/", handleUploadPage).Methods("GET")
	r.HandleFunc("/api/upload", handleUpload).Methods("POST")
	r.HandleFunc("/api/reprocess", handleReprocessAll).Methods("POST")
	r.HandleFunc("/api/status", handleStatus).Methods("GET")
	r.HandleFunc("/api/search", handleSearch).Methods("GET")
	r.HandleFunc("/api/v1/produtos", handleBuscarTributo).Methods("GET")

	log.Printf("🌐 Server running at http://localhost%s", serverPort)
	log.Println("📡 API endpoint: GET /api/v1/produtos?token=...&codigo=NCM&uf=SC&valor=100")
	if err := http.ListenAndServe(serverPort, r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
