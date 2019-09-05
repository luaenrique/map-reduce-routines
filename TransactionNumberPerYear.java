package advanced.transacoes;

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

public class TransactionNumberPerYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);


        // criacao do job e seu nome
        Job j = new Job(c, "transaction-number");

        /*
         *  Registro de classe
         *  Qual é a classe principal que está rodando o main?
         *  -j.setJarByClass(WordCount.class);
         * */
        j.setJarByClass(TransactionNumberPerYear.class);
        j.setMapperClass(TransactionNumberPerYear.MapForTransactionPerYear.class);
        j.setReducerClass(TransactionNumberPerYear.ReduceForTransactionPerYear.class);
        j.setCombinerClass(TransactionNumberPerYear.ReduceForTransactionPerYear.class);


        /*
         * definição dos tipos de saída
         *
         * */
        j.setOutputKeyClass(LongWritable.class);
        j.setOutputValueClass(LongWritable.class);



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
    public static class MapForTransactionPerYear extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

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
            if(!palavras[1].equals("year") && !palavras[1].equals("")) {

                con.write(new LongWritable(Long.parseLong(palavras[1])), new LongWritable(1));
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
    public static class ReduceForTransactionPerYear extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable >{

        // Funcao de reduce
        public void reduce(LongWritable year, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;

            //para cada item da lista de valores é feita uma soma
            for(LongWritable v : values){
                sum += v.get();
            }


            //apresentar o resultado final através de emissao para um arquivo

            con.write(year, new LongWritable(sum));
        }
    }


}
