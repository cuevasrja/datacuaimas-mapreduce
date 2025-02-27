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

En caso de tener una version vieja del proyecto, se puede usar el siguiente comando en el directorio raiz del proyecto:

```bash
mvn clean compile
```

## Caso 1

Para ejecutar el caso 1 se debe ejecutar

```bash
mvn -q exec:java -Dexec.mainClass=com.datacuaimas.avro.GenericMain "src/main/avro/users.avsc"
```

## Caso 2

Para ejecutar el caso 2 se debe ejecutar primero el generador de Users

```bash
mvn -q exec:java -Dexec.mainClass=com.datacuaimas.avro.GenerateData
```

Luego, ejecutar

```bash
mvn exec:java -q -Dexec.mainClass=com.datacuaimas.avro.MapredColorCount -Dexec.args="input output"
```

Para deserilizar el archivo de salida, ejecutar

```bash
mvn exec:java -q -Dexec.mainClass=com.datacuaimas.avro.DeserializationData -Dexec.args="output"
```

## Caso 3

```bash
mvn -q exec:java -Dexec.mainClass=com.datacuaimas.avro.GenerateSpotify -Dexec.args="<flag>"
```

Donde:

- flag: Puede ser "serializer" o "deserializer"
