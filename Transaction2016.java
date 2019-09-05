package advanced.transacoes;

import advanced.transacoes.writables.TransactionsPerCommodityWritable;
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
import org.apache.zookeeper.Transaction;


import java.io.IOException;

public class Transaction2016 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "wordcount-professor");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(Transaction2016.class);
        j.setMapperClass(Transaction2016.MapForTransactionNumber.class);


        /*
         *
         * Utilizando a mesma classe para combiner e reducer, pois ambas seguem o mesmo procedimento
         *
         * */
        j.setReducerClass(Transaction2016.ReduceForTransactionNumber.class);
        j.setCombinerClass(Transaction2016.CombineForTransactionNumber.class);

        /*
         * definição dos tipos de saída
         *
         * */
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(TransactionsPerCommodityWritable.class);



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
    public static class MapForTransactionNumber extends Mapper<LongWritable, Text, Text, TransactionsPerCommodityWritable> {

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
            if(!palavras[8].equals("quantity") && !palavras[8].equals("") && palavras[1].equals("2016")) {
                if (palavras[0].equals("Brazil")){
                    con.write(new Text("1"), new TransactionsPerCommodityWritable(palavras[3], Float.parseFloat(palavras[8])));
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

    public static class CombineForTransactionNumber extends Reducer<Text, TransactionsPerCommodityWritable, Text, TransactionsPerCommodityWritable >{

        // Funcao de reduce
        public void reduce(Text commodity, Iterable<TransactionsPerCommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            float quantity = 0;
            TransactionsPerCommodityWritable maior = null;
            //para cada item da lista de valores é feita uma soma
            for(TransactionsPerCommodityWritable v : values){
                if(quantity < v.getQuantidade()){
                    quantity = v.getQuantidade();
                    maior = v;
                }
            }


            //apresentar o resultado final através de emissao para um arquivo

            con.write(commodity, maior);
        }
    }

    public static class ReduceForTransactionNumber extends Reducer<Text, TransactionsPerCommodityWritable, Text, Text >{

        // Funcao de reduce
        public void reduce(Text commodity, Iterable<TransactionsPerCommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            float quantity = 0;
            String texto = "";
            //para cada item da lista de valores é feita uma soma
            for(TransactionsPerCommodityWritable v : values){
                if(quantity < v.getQuantidade()){
                    quantity = v.getQuantidade();
                    texto    = v.getCommodity();
                }
            }



            //apresentar o resultado final através de emissao para um arquivo
            con.write(new Text(texto), new Text(String.valueOf(quantity)));
        }
    }

}
