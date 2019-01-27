class WorkingFile:

    def printFile(self, ruta, nombre_fichero, modo, lista):
        print('Writting file...')
        salida = ruta + '/' + nombre_fichero
        with open(salida, modo) as fout:
            for elemento in lista:
                fout.write("%s\n" % elemento)
        print('Done')

    def writeFileHDFS(self, path, fileName, mode, dataframe):
        dataframe.write \
            .mode(mode) \
            .save(path + fileName)
