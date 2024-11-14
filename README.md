# TFM
Implementado en Python 3.12.7. Dependencias en requirements.txt.

## Makefile
En el makefile están los principales comandos.

### Crear venv y instalar
Con el comando *init* creamos el venv, instalamos las dependencias y instalamos el proyecto en modo editable para hacer más fácil el desarrollo.
```
make.sh init
```

Una vez creado el venv y instalado los paquetes, lo cargamos con:
```
srouce venv/bin/activate
```

### Ejecutar servidor
Ejecuta el servidor. Ahora mismo se ejecuta en localhost:5000.
```
make.sh server
```
### Ejecutar tests
Se han implementado tests con la libreria *unittest*.
```
make.sh test
```
### Releer los datasets
Hace que el servidor relea los metadatos de los datasets, de forma que se pueden añadir datasets nuevos o modificar su configuración sin necesidad de reiniciar el servidor. Manda la señal SIGHUP al proceso del servidor.
```
make.sh reload
```
