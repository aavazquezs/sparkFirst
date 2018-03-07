package com.aavs.sparkfirst.sql;
import java.util.Arrays;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * Ejemplo de Spark SQL
 * @author angel
 */
public class ExampleSparkSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .config("spark.master", "local")
                .getOrCreate();
        
        /*Leyendo los datos un archivo. Crea un dataset con la estructura especificada por el dataset*/
        //Carga el dataset a partir de un TSV, un archivo de texto separados por tabs.
        Dataset<Row> dataset = spark.read()
                .format("com.databricks.spark.csv")
                .option("delimiter", "\t")
                .option("header", "true")
                .load("/media/Datos/Usuarios/Angel/PhD/Datasets/algebra_2008_2009_test.txt");
        
        dataset.printSchema(); //imprime el schema del dataset
        dataset.show(); //muestra las primeras 20 apariciones en el dataset
        
        spark.stop();
    }
}
