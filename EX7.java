package advanced.transacoes;

import advanced.customwritable.FireAvgTempWritable;
import advanced.transacoes.writables.*;
import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class EX7 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "price-per-weight-unit");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(EX7.class);
        j.setMapperClass(EX7.MapForTransactionWeightValueAvg.class);
        j.setReducerClass(EX7.ReduceForWeightValueAvg.class);
        j.setCombinerClass(EX7.CombineForWeightValueAvg.class);


        /*
         * definição dos tipos de saída
         *
         * */


        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(EX7Writable.class);



        /*
         * Definindo arquivos de entrada e saída
         *
         * */
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // verificando se o primeiro job ja foi executado para executar o segundo
        if(j.waitForCompletion(true)) {


            // criacao do job e seu nome
            Job j2 = new Job(c, "greatest-one");
            // arquivo de entrada
            Path input2 = new Path(files[2]);

            // arquivo de saida
            Path output2 = new Path(files[3]);
            /*
             *  Registro de classe
             *  Qual é a classe principal que está rodando o main?
             *  -j.setJarByClass(WordCount.class);
             * */
            j2.setJarByClass(EX7.class);
            j2.setMapperClass(EX7.MapForOutputFile.class);
            j2.setReducerClass(EX7.ReduceForOutputFile.class);
            j2.setCombinerClass(EX7.CombineForOutputFile.class);


            /*
             * definição dos tipos de saída
             *
             * */
            j2.setMapOutputKeyClass(Text.class);
            j2.setMapOutputValueClass(TransactionsPerCommodityWritable.class);
            j2.setOutputKeyClass(Text.class);
            j2.setOutputValueClass(FloatWritable.class);



            /*
             * Definindo arquivos de entrada e saída
             *
             * */
            FileInputFormat.addInputPath(j2, input2);
            FileOutputFormat.setOutputPath(j2, output2);

            // lanca o job e aguarda sua execucao
            System.exit(j2.waitForCompletion(true) ? 0 : 1);
        }
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
    public static class MapForTransactionWeightValueAvg extends Mapper<LongWritable, Text, Text, EX7Writable> {

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

            //verificando se é o cabeçalho
            if(!palavras[1].equals("year") && !palavras[1].equals("") && !palavras[8].equals("") && !palavras[6].equals("")) {
                /*
                *  Chave: simples - commodity
                *  Valor: composto - custom writable - preço e peso.
                * */
                con.write(new Text(palavras[3]), new EX7Writable(Float.parseFloat(palavras[5]), Float.parseFloat(palavras[6])));
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
    public static class CombineForWeightValueAvg extends Reducer<Text, EX7Writable, Text, EX7Writable >{

        // Funcao de reduce
        public void reduce(Text key, Iterable<EX7Writable> values, Context con)
                throws IOException, InterruptedException {
            float valor = 0;
            float peso = 0;

            //somando todos os valores e pesos
            for(EX7Writable v : values){
                valor += v.getValor();
                peso += v.getPeso();
            }


            con.write(key, new EX7Writable(valor, peso));
        }
    }

    public static class ReduceForWeightValueAvg extends Reducer<Text, EX7Writable, Text, Text >{

        // Funcao de reduce
        public void reduce(Text key, Iterable<EX7Writable> values, Context con)
                throws IOException, InterruptedException {
            float valor = 0;
            float peso = 0;


            for(EX7Writable v : values){
                valor += v.getValor();
                peso += v.getPeso();
            }

            // calculando média
            float valorFinal = valor/peso;

            //emissao em arquivo separando por ; para utilizar no segundo map
            con.write(key, new Text(";"+String.valueOf(valorFinal)));

        }
    }


    public static class MapForOutputFile extends Mapper<LongWritable, Text, Text, TransactionsPerCommodityWritable> {

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

            //quebrando em  ;
            String[] palavras = linha.split(";");

            //verificando se está vazio
            if(!palavras[0].equals("")) {
                /*
                *  chave: comum para todos
                *  valor: composto custom writable - commodity, quantidade.
                * */
                con.write(new Text("1"), new TransactionsPerCommodityWritable(palavras[0], Float.parseFloat(palavras[1])));
            }
        }
    }

    public static class CombineForOutputFile extends Reducer<Text, TransactionsPerCommodityWritable, Text, TransactionsPerCommodityWritable >{

        // Funcao de reduce
        public void reduce(Text key, Iterable<TransactionsPerCommodityWritable> values, Context con)
                throws IOException, InterruptedException {


            float valor = 0;
            String commodity = "";

            //verificando qual commodity é a maior
            for(TransactionsPerCommodityWritable v : values){
                if(valor < v.getQuantidade() && !String.valueOf(v.getQuantidade()).equals("Infinity")) {
                    valor = v.getQuantidade();
                    commodity = String.valueOf(v.getCommodity());
                }
            }


            con.write(new Text("1"), new TransactionsPerCommodityWritable(commodity, valor));

        }
    }

    public static class ReduceForOutputFile extends Reducer<Text, TransactionsPerCommodityWritable, Text, FloatWritable >{

        // Funcao de reduce
        public void reduce(Text key, Iterable<TransactionsPerCommodityWritable> values, Context con)
                throws IOException, InterruptedException {


            float valor = 0;
            String commodity = "";

            //verificando  qual mercadoria é com maior quantidade
            for(TransactionsPerCommodityWritable v : values){
                if(valor < v.getQuantidade() && !String.valueOf(v.getQuantidade()).equals("Infinity")) {
                    valor = v.getQuantidade();
                    commodity = String.valueOf(v.getCommodity());
                }
            }


            con.write(new Text(commodity), new FloatWritable((valor)));

        }
    }

}
