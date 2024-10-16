# TFM
Implementado en Python 3.12.6. Dependencias en requirements.txt.


## Makefile
En el makefile están los principales comandos.

### Ejecutar servidor
```
make server
```
### Ejecutar tests
Se han implementado tests con la libreria *unittest*.
```
make test
```
### Releer los datasets
Hace que el servidor relea los metadatos de los datasets, de forma que se pueden añadir datasets nuevos o modificar su configuración sin necesidad de reiniciar el servidor. Manda la señal SIGHUP al proceso del servidor.
```
make reload
```

## Ficheros importantes
### server.py
Implementa la API HTTP.

### server_init.py
Implementa la lectura de los metadatos de cada dataset.

### client.py
Implementa el cliente para acceder a la API.

### test.py
Tests principales

### test_init.py
Algunos test para comprobar que se leen bien los metadatos de los datasets.

### requirements.txt
Dependencias. Ahora mismo tiene más de las que necesita, hace falta limpiarlo (TODO).
```
pip install -r requirements.txt
```
