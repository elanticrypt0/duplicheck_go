package explorer

import (
	"crypto/sha256"
	"duplicheck/models"
	"duplicheck/pkg/utils"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

// Agregar esta estructura al inicio del archivo, junto a Explorer
type Progress struct {
	TotalFiles     int64
	ProcessedFiles int64
	mutex          sync.Mutex
}

type Explorer struct {
	DB         *gorm.DB
	MaxWorkers int
	BatchSize  int
	filesChan  chan models.File
	errorsChan chan error
	wg         sync.WaitGroup
	progress   Progress
	updateTick time.Duration // Frecuencia de actualización del progreso
}

func New(db *gorm.DB) *Explorer {
	return &Explorer{
		DB:         db,
		MaxWorkers: 5,
		BatchSize:  100,
		filesChan:  make(chan models.File, 1000),
		errorsChan: make(chan error, 100),
		updateTick: 500 * time.Millisecond, // Actualiza cada medio segundo
	}
}

// Agregar este nuevo método para actualizar el progreso
func (p *Progress) increment() {
	p.mutex.Lock()
	p.ProcessedFiles++
	current := float64(p.ProcessedFiles) / float64(p.TotalFiles) * 100
	fmt.Printf("\rProgreso: %.2f%% (%d/%d archivos)", current, p.ProcessedFiles, p.TotalFiles)
	p.mutex.Unlock()
}

func (e *Explorer) Scan(path2scan string) {
	startTime := time.Now()
	fmt.Printf("Iniciando escaneo de: %s\n", path2scan)

	// Contar archivos primero
	var err error
	e.progress.TotalFiles, err = e.countFiles(path2scan)
	if err != nil {
		fmt.Printf("Error contando archivos: %v\n", err)
		return
	}

	fmt.Printf("Total de archivos a procesar: %d\n", e.progress.TotalFiles)

	// Inicia los workers para procesar archivos
	for i := 0; i < e.MaxWorkers; i++ {
		e.wg.Add(1)
		go e.processFiles()
	}

	// Inicia el worker de manejo de errores
	go e.handleErrors()

	// Inicia el escaneo de archivos
	if err := e.traverseDirectory(path2scan); err != nil {
		fmt.Printf("Error en el escaneo inicial: %v\n", err)
		return
	}

	// Cierra el canal de archivos cuando termine el escaneo
	close(e.filesChan)

	// Espera a que todos los workers terminen
	e.wg.Wait()
	close(e.errorsChan)

	elapsed := time.Since(startTime)
	fmt.Printf("Escaneo completado en %s\n", elapsed)
}

// Agregar este nuevo método para contar archivos
func (e *Explorer) countFiles(dir string) (int64, error) {
	var count int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	return count, err
}

func (e *Explorer) processFiles() {
	defer e.wg.Done()

	batch := make([]models.File, 0, e.BatchSize)

	for file := range e.filesChan {
		batch = append(batch, file)

		if len(batch) >= e.BatchSize {
			if err := e.saveBatch(batch); err != nil {
				e.errorsChan <- fmt.Errorf("error guardando lote: %w", err)
			}
			e.progress.increment()
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := e.saveBatch(batch); err != nil {
			e.errorsChan <- fmt.Errorf("error guardando último lote: %w", err)
		}
		e.progress.increment()
	}
}

func (e *Explorer) saveBatch(files []models.File) error {
	return e.DB.Transaction(func(tx *gorm.DB) error {
		for _, file := range files {
			mfile := models.NewFile()
			// Verifica si el hash ya existe antes de crear
			// var count int64
			// if err := tx.Model(&models.File{}).Where("hash = ?", file.Hash).Count(&count).Error; err != nil {
			// 	return err
			// }
			// if count == 0 {
			// 	if err := mfile.Create(tx, file); err != nil {
			// 		return err
			// 	}
			// }

			if err := mfile.Create(tx, file); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *Explorer) handleErrors() {
	for err := range e.errorsChan {
		fmt.Printf("Error durante el escaneo: %v\n", err)
		// Aquí podrías implementar logging a un archivo
	}
}

func (e *Explorer) traverseDirectory(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			e.errorsChan <- fmt.Errorf("error accediendo a %s: %w", path, err)
			return nil // Continúa con el siguiente archivo
		}

		if !info.IsDir() {
			fileInfo, err := e.getFileInfo(path)
			if err != nil {
				e.errorsChan <- fmt.Errorf("error procesando %s: %w", path, err)
				return nil
			}

			if fileInfo.Size > 0 { // Solo procesa archivos no vacíos
				e.filesChan <- fileInfo
			}
		}
		return nil
	})
}

func (e *Explorer) getFileInfo(path string) (models.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return models.File{}, fmt.Errorf("error abriendo archivo: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return models.File{}, fmt.Errorf("error obteniendo stats: %w", err)
	}

	if info.IsDir() {
		return models.File{}, nil
	}

	checksum, err := e.GetFileHash(file)
	if err != nil {
		return models.File{}, fmt.Errorf("error calculando hash: %w", err)
	}

	ext := filepath.Ext(info.Name())
	if ext != "" {
		ext = ext[1:] // Eliminar el punto
	}

	return models.File{
		Name:       info.Name(),
		Ext:        ext,
		Filetype:   utils.GetFiletype(ext),
		Size:       uint(info.Size()),
		Path:       filepath.Dir(path),
		Hash:       checksum,
		HasPreview: false,
	}, nil
}

func (e *Explorer) GetFileHash(file *os.File) (string, error) {
	// Regresa al inicio del archivo después de calcular el hash
	if _, err := file.Seek(0, 0); err != nil {
		return "", fmt.Errorf("error reseteando posición del archivo: %w", err)
	}

	hash := sha256.New()
	if _, err := file.WriteTo(hash); err != nil {
		return "", fmt.Errorf("error calculando hash: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (e *Explorer) cutPath(fullPath, subdirectory string) string {
	index := strings.Index(fullPath, subdirectory)
	if index != -1 {
		return fullPath[index:]
	}
	return fullPath
}
