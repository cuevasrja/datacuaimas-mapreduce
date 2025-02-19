# Apache Avro

## Instalacion

Para instalar las librerias de avro en Maven, se puede usar el siguiente comando en el directorio raiz del proyecto:

```bash
mvn install
```

## Uso

Para usar las librerias de avro en Maven, se puede usar el siguiente comando en el directorio raiz del proyecto:

```bash
mvn compile
```

Para ejecutar el programa, se puede usar el siguiente comando en el directorio raiz del proyecto:

```bash
mvn -e exec:java -Dexec.mainClass=com.datacuaimas.avro.Main -Dexec.args='avro_user pat Hello_World'
```

Donde:
- exec:java: Ejecuta el programa
- -Dexec.mainClass=com.datacuaimas.avro.Main: Clase principal del programa
- -Dexec.args='avro_user pat Hello_World': Argumentos del programa (Opconal y depende de la clase principal)