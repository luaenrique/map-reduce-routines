package advanced.transacoes;

import advanced.transacoes.writables.TransactionsPerCommodityWritable;
import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class FASTA {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "fasta-lua");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(FASTA.class);
        j.setMapperClass(FASTA.MapForTransactionNumber.class);


        /*
         *
         * Utilizando a mesma classe para combiner e reducer, pois ambas seguem o mesmo procedimento
         *
         * */
         j.setReducerClass(FASTA.ReduceForTransactionNumber.class);
         j.setCombinerClass(FASTA.CombinerForTransactionNumber.class);

        /*
         * definição dos tipos de saída
         *
         * */
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);



        /*
         * Definindo arquivos de entrada e saída
         *
         * */
         FileInputFormat.addInputPath(j, input);
         FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
         if(j.waitForCompletion(true)) {


            // criacao do job e seu nome
            Job j2 = new Job(c, "fasta-entrop");
            // arquivo de entrada
            Path input2 = new Path(files[2]);

            // arquivo de saida
            Path output2 = new Path(files[3]);
            /*
             *  Registro de classe
             *  Qual é a classe principal que está rodando o main?
             *  -j.setJarByClass(WordCount.class);
             * */
            j2.setJarByClass(FASTA.class);
            j2.setMapperClass(FASTA.MapForFastaOutputFile.class);
            j2.setReducerClass(FASTA.ReduceForFastaOutputFile.class);
            j2.setCombinerClass(FASTA.CombineForFastaOutputFile.class);


            /*
             * definição dos tipos de saída
             *
             * */
            j2.setMapOutputKeyClass(Text.class);
            j2.setMapOutputValueClass(LongWritable.class);
            j2.setOutputKeyClass(Text.class);
            j2.setOutputValueClass(DoubleWritable   .class);



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
    public static class MapForTransactionNumber extends Mapper<LongWritable, Text, Text, LongWritable> {

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
            //String[] palavras = linha.split("|");

            //verificando se a linha em questão é o cabeçalho do csv
            if(!linha.contains("gi") && !linha.equals("")) {
                for(int i = 0; i < linha.length(); i++){
                    if(!String.valueOf(linha.charAt(i)).replaceAll("\\s+","").equals("")) {
                        con.write(new Text(String.valueOf(linha.charAt(i))), new LongWritable(1));
                    }
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
     * */

    public static class CombinerForTransactionNumber extends Reducer<Text, LongWritable, Text, LongWritable >{

        // Funcao de reduce
        public void reduce(Text letra, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }


            con.write(letra, new LongWritable(sum));
        }
    }


    public static class ReduceForTransactionNumber extends Reducer<Text, LongWritable, Text, LongWritable >{

        // Funcao de reduce
        public void reduce(Text letra, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }


            con.write(new Text(String.valueOf(letra).trim()+";"),new LongWritable(Long.parseLong(String.valueOf(sum).trim())));
        }
    }

    public static class MapForFastaOutputFile extends Mapper<LongWritable, Text, Text, LongWritable> {
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
            String linha = value.toString().replaceAll("\\s+","");


            //quebrando em palavras
            String[] palavras = linha.split(";");

            //verificando se a linha em questão é o cabeçalho do csv
            if(!linha.trim().equals("")) {
                con.write(new Text(palavras[0]), new LongWritable(Long.parseLong(palavras[1].trim())));
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
     * */
    public static class CombineForFastaOutputFile extends Reducer<Text, LongWritable, Text, LongWritable >{

        // Funcao de reduce
        public void reduce(Text letra, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }

            //apresentar o resultado final de acordo com a commodity
            con.write(letra, new LongWritable(sum));
        }
    }

    public static class ReduceForFastaOutputFile extends Reducer<Text, LongWritable, Text, DoubleWritable>{

        // Funcao de reduce
        public void reduce(Text letra, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            Double sum = 0.0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }

            Double p  = sum/248956422;

            Double entropia = -(p) * Math.log10(p)/Math.log10(2);

            //apresentar o resultado final de acordo com a commodity
            con.write(letra, new DoubleWritable(entropia));
        }
    }

}
