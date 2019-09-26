package advanced.transacoes;

import advanced.customwritable.FireAvgTempWritable;
import advanced.transacoes.writables.TransactionWeightAvgWritableKey;
import advanced.transacoes.writables.TransactionsWeightAvgWritable;
import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class EX5 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "weight-avg");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(EX5.class);
        j.setMapperClass(EX5.MapForTransactionWeightAvg.class);
        j.setReducerClass(EX5.ReduceForWeightAvg.class);
        j.setCombinerClass(EX5.CombineForWeightAvg.class);


        /*
         * definição dos tipos de saída
         *
         * */

        j.setMapOutputKeyClass(TransactionWeightAvgWritableKey.class);
        j.setMapOutputValueClass(TransactionsWeightAvgWritable.class);
        j.setOutputKeyClass(TransactionWeightAvgWritableKey.class);
        j.setOutputValueClass(TransactionsWeightAvgWritable.class);



        /*
         * Definindo arquivos de entrada e saída
         *
         * */
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    /*
     * Map
     *
     * Mapper<Tipo da chave de entrada, tipo do valor de entrada,
     *         Tipo da chave de saída, Tipo do valor de saída>
     *   Mapper< id arquivo, texto do arquivo, chave, valor>
     *       se for arquivo de texto LongWritable e Text nunca mudam
     *
     * */
    public static class MapForTransactionWeightAvg extends Mapper<LongWritable, Text, TransactionWeightAvgWritableKey, TransactionsWeightAvgWritable> {

        // Funcao de map
        /*
         * Value = cada linha
         *
         *
         * Context é responsável por enviar o map para o reduce
         * */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //capturando as linhas
            String linha = value.toString();

            //quebrando em palavras
            String[] palavras = linha.split(";");

            //verificando se é a linha do cabeçalho do csv
            if(!palavras[1].equals("year") && !palavras[1].equals("") && !palavras[8].equals("") && !palavras[6].equals("")) {
                //verificando se o país é o Brasil.
                if (palavras[0].equals("Brazil")) {
                    /*
                    *  Chave composta: commodity e ano
                    *  Valor composto: quantidade e peso
                    * */
                    con.write(new TransactionWeightAvgWritableKey(palavras[3], palavras[1]), new TransactionsWeightAvgWritable(Float.parseFloat(palavras[8]), Float.parseFloat(palavras[6])));
                }
            }
        }
    }

    /*
     *
     * Reduce
     *
     * Reducer<Tipo da chave de entrada, tipo do valor de entrada -> saída do map!
     *           Tipo da chave de saída, tipo do valor de saída>
     *          <Palavra, ocorrencias,
     *            ...>
     *
     *
     *
     *
     * */
    public static class CombineForWeightAvg extends Reducer<TransactionWeightAvgWritableKey, TransactionsWeightAvgWritable, TransactionWeightAvgWritableKey, TransactionsWeightAvgWritable >{

        // Funcao de reduce
        public void reduce(TransactionWeightAvgWritableKey key, Iterable<TransactionsWeightAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float qtd = 0;
            float peso = 0;

            //somando todas as quantidades e pesos
            for(TransactionsWeightAvgWritable v : values){
                qtd += v.getQuantidade();
                peso += v.getPeso();
            }


            //apresentar o resultado final através de emissao para um arquivo

            con.write(key, new TransactionsWeightAvgWritable(qtd, peso));
        }
    }

    public static class ReduceForWeightAvg extends Reducer<TransactionWeightAvgWritableKey, TransactionsWeightAvgWritable, Text, Text >{

        // Funcao de reduce
        public void reduce(TransactionWeightAvgWritableKey key, Iterable<TransactionsWeightAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float media = 0;

            //para cada item da lista de valores é feito o calculo de média
            for(TransactionsWeightAvgWritable v : values){
                media += v.getPeso()/v.getQuantidade();
            }

            //printando no arquivo
            con.write(new Text(key.getYear()+" "+key.getCommodity()), new Text(String.valueOf(media)));

        }
    }
}
