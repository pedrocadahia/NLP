# execfile(PATH + "syntax/scripts/carga_todas_funciones.py")

from os import listdir

allFiles = listdir(PATH + "syntax/scripts/NER/")

filesExec = [filename for filename in allFiles if ".py" in filename and filename != "carga_todas_funciones.py"]
filesExec.sort()

for filename in reversed(filesExec):
  execfile(PATH + "syntax/scripts/NER/" + filename)

