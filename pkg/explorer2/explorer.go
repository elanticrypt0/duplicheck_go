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

	"gorm.io/gorm"
)

type Explorer struct {
	DB *gorm.DB
}

func New(db *gorm.DB, scriptsPath string) *Explorer {
	exp := &Explorer{}
	exp.DB = db
	return exp
}

func (me *Explorer) Scan(path2scan string) {
	// Define un WaitGroup
	files, err := me.traverseDirectory(path2scan)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// store files into db
	for _, file := range files {
		fmt.Printf("Nombre: %s, Extensión: %s, Tamaño: %d bytes, Carpeta: %s, Checksum: %s\n",
			file.Name, file.Ext, file.Size, file.Path, file.Hash)
		mfile := models.NewFile()

		mfile.Create(me.DB, file)
		// if !mfile.IsHashfile(me.DB, file.Hash) {
		// 	mfile.Create(me.DB, file)
		// }
	}

	// fmt.Println(">> Capturas de pantalla completadas.") // Mensaje al finalizar
}

func (me *Explorer) traverseDirectory(dir string) ([]models.File, error) {

	var files []models.File

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			fileInfo, err := me.getFileInfo(path)
			if err != nil {
				return err
			}
			files = append(files, fileInfo)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (me *Explorer) getFileInfo(path string) (models.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return models.File{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return models.File{}, err
	}

	if info.IsDir() {
		return models.File{}, nil
	}

	checksum, err := me.GetFileHash(file)
	if err != nil {
		return models.File{}, err
	}

	ext := filepath.Ext(info.Name())
	if ext != "" {
		ext = ext[1:] // Eliminar el punto
	}

	fileOutput := models.File{
		Name:     info.Name(),
		Ext:      ext,
		Filetype: utils.GetFiletype(ext),
		Size:     uint(info.Size()),
		// Path:       me.cutPath(filepath.Dir(path), "storage"),
		Path:       filepath.Dir(path),
		Hash:       checksum,
		HasPreview: false,
	}

	return fileOutput, nil
}

func (me *Explorer) GetFileHash(file *os.File) (string, error) {
	hash := sha256.New()
	if _, err := file.WriteTo(hash); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// cutPath corta la ruta completa a partir del subdirectorio especificado
func (me *Explorer) cutPath(fullPath, subdirectory string) string {
	index := strings.Index(fullPath, subdirectory)
	if index != -1 {
		return fullPath[index:]
	}
	return fullPath // Si no se encuentra el subdirectorio, se devuelve la ruta completa
}
