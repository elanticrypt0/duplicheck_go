package main

import (
	"duplicheck/models"
	"duplicheck/pkg/explorer"
	"duplicheck/pkg/utils"
	"flag"
	"fmt"

	"github.com/elanticrypt0/dbman"
)

func main() {

	AppBanner()

	db := connectDB()

	dir_flag := flag.String("d", "", "Ruta al directorio a analizar")
	show_duplicates_flag := flag.Bool("show", false, "Muestra los archivos duplicados")
	count_duplicates_flag := flag.Bool("count", false, "Cuenta la cantidad de archivos duplicados")
	find_hash_flag := flag.String("find", "", "Busca un archivo duplicado por su hash")
	// copy_duplicates_flag := flag.String("-copy", "", "Copia los archivos duplicados")
	// copy_duplicates_destiny_flag := flag.String("dst", "", "Ruta de las copias")
	// count := flag.String("count", "", "Contar la cantidad de archivos procesados")

	flag.Parse()

	fmt.Printf("\n")

	if *dir_flag != "" {
		working_dir := *dir_flag

		fmt.Printf("Carpeta a analizar: %q", working_dir)

		fmt.Printf("scanning...\n")
		exp := explorer.New(db.Primary)

		exp.Scan(working_dir)

		return
	}

	if *show_duplicates_flag != false {
		mfile := models.NewFile()

		// files := &[]models.File

		files, _ := mfile.FindDuplicateHashes(db.Primary)

		i := 0
		for _, file := range files {
			i++
			// fmt.Printf("%02d - %q - Hash: %q \n", i, file.Name, file.Hash)
			msgFile := fmt.Sprintf("%q -HASH %s -PATH %s", file.Name, file.Hash, file.Path)
			msgPrefix := fmt.Sprintf("%02d", i)
			PrintFile(msgPrefix, msgFile)
		}

		return
	}

	if *count_duplicates_flag != false {
		mfile := models.NewFile()

		qty, _ := mfile.CountDuplicateHashes(db.Primary)

		msgFile := fmt.Sprintf("%02d", qty)
		msgPrefix := fmt.Sprintf("%s", "Archivos duplicados en total:")
		PrintFile(msgPrefix, msgFile)

		return
	}

	if *find_hash_flag != "" {

		hash := *find_hash_flag

		mfile := models.NewFile()

		// files := &[]models.File

		files := mfile.FindByHash(db.Primary, hash)

		i := 0
		for _, file := range files {
			i++
			// fmt.Printf("%02d - %q - Hash: %q \n", i, file.Name, file.Hash)
			msgFile := fmt.Sprintf("%q -HASH %s -PATH %s", file.Name, file.Hash, file.Path)
			msgPrefix := fmt.Sprintf("%02d", i)
			PrintFile(msgPrefix, msgFile)
		}

		return
	}

	// if *copy_duplicates_flag != "" {

	// 	destinyPath := *copy_duplicates_flag

	// 	mfile := models.NewFile()

	// 	// files := &[]models.File

	// 	files := mfile.FindByHash(db.Primary, hash)

	// 	i := 0
	// 	for _, file := range files {
	// 		i++
	// 		// fmt.Printf("%02d - %q - Hash: %q \n", i, file.Name, file.Hash)
	// 		msgFile := fmt.Sprintf("%q -HASH %s -PATH %s", file.Name, file.Hash, file.Path)
	// 		msgPrefix := fmt.Sprintf("%02d", i)
	// 		PrintFile(msgPrefix, msgFile)
	// 	}

	// 	return
	// }

}

func connectDB() *dbman.DBMan {
	db := dbman.New()
	dbConfigPath := utils.GetAppRootPath() + "config/db_config.toml"
	fmt.Printf("DB config path: %q\n", dbConfigPath)
	db.SetRootPath("./")
	db.LoadConfigToml(dbConfigPath)
	db.Connect("local")
	db.SetPrimary("local")

	db.Primary.AutoMigrate(&models.File{}, &models.Tag{})

	return db
}
